#pragma once
/*
Per-thread EventLoop.

A Node<N> spawns N worker threads, each running its own EventLoop. Each
loop owns:
  - One listening socket bound to the same TCP port via SO_REUSEPORT;
    the kernel hashes incoming connection 4-tuples to one of the N
    accept queues, so different inbound clients land on different
    threads with no shared lock.
  - One eventfd, used to wake the loop from epoll_wait when another
    thread posts an outbound RPC into this loop's inbox.
  - One epoll instance covering the listen fd, the eventfd, all
    accepted client fds, and all outbound peer fds for this loop's
    peer subset (peer_id % N == thread index).

Inbound flow:
  Accept -> ClientConnSlab::Acquire -> register fd EPOLLIN | EPOLLRDHUP
  -> OnReadable parses requests from rbuf via try_parse_req ->
  RpcHandlers dispatches synchronously into Node-side Raft handlers
  (which take state_mu_) -> the response is serialized into wbuf and
  EPOLLOUT is armed -> OnWritable drains wbuf.

Outbound flow:
  Node calls EnqueueAE/RV/IS on the owning loop. The request is
  serialized into a temp buffer, packaged with a reply callback, and
  pushed onto inbox_ under a mutex; the loop is woken by writing to
  eventfd. OnEventfd drains the inbox, lazily kicks a non-blocking
  connect for any peer in Disconnected, appends bytes to peer.wbuf,
  pushes the PendingReply onto peer.inflight, and arms EPOLLOUT.
  Connection completion arrives as EPOLLOUT in Connecting; SO_ERROR
  is checked. Once Connected, OnPeerWritable drains; OnPeerReadable
  calls try_parse_*_resp in inflight-FIFO order and fires the
  corresponding PendingReply callback on the loop thread.
*/

#include "./conns.hpp"
#include "./payloads.hpp"
#include "./protocol.hpp"
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

inline constexpr int    EPOLL_BATCH    = 64;
inline constexpr size_t RECV_CHUNK     = 4096;

// struct PeerEndpoint {
//     NodeID      id;
//     const char* ip;
//     const char* port;
// };

// Peer endpoints in stable storage. Indexed by NodeID.
struct PeerInfo {
    NodeID      id;
    const char* ip;
    const char* port;
};

// RpcHandlers and the per-direction dispatch tables (kRpcEntries,
// kReplyEntries) live in protocol.hpp so they can be reused by other
// callers and so the id-to-action mapping has a single source of truth.

struct EventLoop {
    EventLoop(FD listen_fd,
              std::vector<PeerInfo> peers,
              RpcHandlers handlers,
              size_t inbound_cap);
    ~EventLoop();

    EventLoop(const EventLoop&)            = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Run();
    void Stop();

    // Cross-thread: serialize the request, package with reply callback,
    // push onto inbox, wake loop. Safe to call from any thread.
    void EnqueueAE(NodeID peer_id,
                   AppendEntriesReqPayload payload,
                   std::function<void(AppendEntriesRespPayload)> on_reply);
    void EnqueueRV(NodeID peer_id,
                   RequestVoteReqPayload payload,
                   std::function<void(RequestVoteRespPayload)> on_reply);
    void EnqueueIS(NodeID peer_id,
                   InstallSnapshotReqPayload payload,
                   std::function<void(InstallSnapshotRespPayload)> on_reply);

private:
    // ---- fds and lifecycle -----
    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd  = -1;
    std::atomic<bool> stopped{false};

    RpcHandlers handlers;

    // ---- inbound ----
    ClientConnSlab                            client_slab;
    std::unordered_map<ClientID, ClientConn*> conns;
    std::unordered_map<FD, ClientID>          fd_to_id;
    //std::unordered_set<ClientID>              stalled;
    ClientID next_conn_id = 1;

    // ---- outbound ----
    std::unordered_map<NodeID, PeerConn> peer_conns;
    std::unordered_map<FD, NodeID>       peer_fd_to_id;

    // ---- cross-thread inbox ----
    struct Outbound {
        std::vector<std::byte> bytes;
        PendingReply reply;
        NodeID peer_id;
    };
    std::mutex inbox_mu;
    std::vector<Outbound> inbox;

    // ---- helpers ----
    static void set_nonblocking(FD fd);
    void register_fd(FD fd, uint32_t events);
    void modify_client_interest(ClientConn& c, uint32_t events);
    void modify_peer_interest(PeerConn& p, uint32_t events);
    void wake();

    // inbound messaging
    void Accept();
    void OnReadable(ClientConn& c);
    void OnWritable(ClientConn& c);
    void DispatchOneRequest(ClientConn& c);
    void CloseClient(ClientConn& c);
    void ReapClient(ClientConn& c);

    // outbound messaging
    void OnPeerWritable(PeerConn& p);
    void OnPeerReadable(PeerConn& p);
    void StartConnect(PeerConn& p);
    void DropPeer(PeerConn& p);

    // wake / inbox
    void OnEventfd();
    void DrainInbox();
};

// =============================================================================
// Implementation
// =============================================================================

inline EventLoop::EventLoop(FD listen_fd_,
                            std::vector<PeerInfo> peers,
                            RpcHandlers handlers_,
                            size_t inbound_cap)
    : listen_fd(listen_fd_),
      handlers(std::move(handlers_)),
      client_slab(inbound_cap) {
    epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) throw std::runtime_error("epoll_create1 failed");

    event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd < 0) throw std::runtime_error("eventfd failed");

    set_nonblocking(listen_fd);
    register_fd(listen_fd, EPOLLIN);
    register_fd(event_fd,  EPOLLIN);

    peer_conns.reserve(peers.size());
    for (const auto& pe : peers) {
        PeerConn pc;
        pc.peer_id = pe.id;
        pc.ip      = pe.ip;
        pc.port    = pe.port;
        peer_conns.emplace(pe.id, std::move(pc));
    }
}

inline EventLoop::~EventLoop() {
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }
    // Listen fd is owned by Node.
}

inline void EventLoop::Stop() {
    stopped.store(true, std::memory_order_release);
    wake();
}

inline void EventLoop::wake() {
    uint64_t one = 1;
    ssize_t n = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

inline void EventLoop::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
    }
}

inline void EventLoop::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl ADD failed");
    }
}

inline void EventLoop::modify_client_interest(ClientConn& c, uint32_t events) {
    if (c.epoll_events == events) return;
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = c.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c.fd, &ev) < 0) {
        CloseClient(c);
        return;
    }
    c.epoll_events = events;
}

inline void EventLoop::modify_peer_interest(PeerConn& p, uint32_t events) {
    if (p.epoll_events == events) return;
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = p.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, p.fd, &ev) < 0) {
        DropPeer(p);
        return;
    }
    p.epoll_events = events;
}

// ---- main run loop ----------------------------------------------------------

inline void EventLoop::Run() {
    epoll_event evs[EPOLL_BATCH];
    while (!stopped.load(std::memory_order_acquire)) {
        int n = ::epoll_wait(epoll_fd, evs, EPOLL_BATCH, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("epoll_wait failed");
        }

        for (int i = 0; i < n; ++i) {
            const FD       fd = evs[i].data.fd;
            const uint32_t e  = evs[i].events;

            if (fd == listen_fd) { Accept(); continue; }
            if (fd == event_fd)  { OnEventfd(); continue; }

            if (auto it = fd_to_id.find(fd); it != fd_to_id.end()) {
                auto cit = conns.find(it->second);
                if (cit == conns.end()) continue;
                ClientConn& c = *cit->second;
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    CloseClient(c);
                    continue;
                }
                if (e & EPOLLIN)  OnReadable(c);
                if (e & EPOLLOUT) OnWritable(c);
                continue;
            }

            if (auto it = peer_fd_to_id.find(fd); it != peer_fd_to_id.end()) {
                PeerConn& p = peer_conns.at(it->second);
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    DropPeer(p);
                    continue;
                }
                if (e & EPOLLOUT) OnPeerWritable(p);
                if (e & EPOLLIN)  OnPeerReadable(p);
                continue;
            }
        }
    }
}

// =============================================================================
// Inbound path
// =============================================================================

inline void EventLoop::Accept() {
    for (;;) {
        sockaddr_in peer{};
        socklen_t   plen = sizeof(peer);
        FD fd = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer),
                          &plen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            return; // transient errors: drop and try again on next epoll wake
        }

        int yes = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        ClientConn* c = client_slab.Acquire();
        if (!c) {
            // Hard cap: refuse the connection rather than allocate.
            ::close(fd);
            continue;
        }
        c->fd = fd;
        c->id = next_conn_id++;
        c->epoll_events = EPOLLIN | EPOLLRDHUP;

        try {
            register_fd(fd, c->epoll_events);
        } catch (const std::exception&) {
            ::close(fd);
            client_slab.Release(c);
            continue;
        }
        fd_to_id[fd] = c->id;
        conns.emplace(c->id, c);
    }
}

inline void EventLoop::OnReadable(ClientConn& c) {
    // obtaining the raw bytes from the wire
    // can possibly yield incomplete message frames
    std::byte tmp[RECV_CHUNK];
    for (;;) {
        ssize_t n = ::recv(c.fd, tmp, sizeof(tmp), 0);
        if (n > 0) {
            c.rbuf.insert(c.rbuf.end(), tmp, tmp + n);
            continue;
        }
        if (n == 0) { CloseClient(c); return; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        CloseClient(c);
        return;
    }

    // Drain as many _complete_ frames as the buffer holds.
    while (!c.closing && !c.rbuf.empty()) {
        size_t before = c.rbuf.size();
        DispatchOneRequest(c); // invoke corresponding Raft handler
        if (c.closing) return;
        if (c.rbuf.size() == before) break; // need more bytes
    }
}

inline void EventLoop::DispatchOneRequest(ClientConn& c) {
    size_t consumed = 0;
    bool parsed = false;
    try {
        parsed = try_parse_and_handle_req(
            c.rbuf.data(), c.rbuf.size(), &consumed, c.wbuf, handlers);
    } catch (const std::exception&) {
        // Malformed frame or handler threw; drop the connection.
        CloseClient(c);
        return;
    }

    if (!parsed) return; // need more bytes

    ++c.next_seq;
    c.rbuf.erase(c.rbuf.begin(), c.rbuf.begin() + consumed);

    if (c.wbuf_offset < c.wbuf.size()) {
        modify_client_interest(c, c.epoll_events | EPOLLOUT);
    }
}

inline void EventLoop::OnWritable(ClientConn& c) {
    while (c.wbuf_offset < c.wbuf.size()) {
        ssize_t n = ::send(c.fd,
                           c.wbuf.data() + c.wbuf_offset,
                           c.wbuf.size() - c.wbuf_offset,
                           MSG_NOSIGNAL);
        if (n > 0) { c.wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        CloseClient(c);
        return;
    }
    c.wbuf.clear();
    c.wbuf_offset = 0;
    modify_client_interest(c, c.epoll_events & ~EPOLLOUT);
    if (c.closing && c.pending_tasks == 0) ReapClient(c);
}

inline void EventLoop::CloseClient(ClientConn& c) {
    if (c.closing) return;
    c.closing = true;

    if (c.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c.fd, nullptr);
        ::close(c.fd);
        fd_to_id.erase(c.fd);
        c.fd = -1;
    }
    c.epoll_events = 0;
    // stalled.erase(c.id);

    if (c.pending_tasks == 0) ReapClient(c);
}

inline void EventLoop::ReapClient(ClientConn& c) {
    const ClientID id = c.id;
    ClientConn* ptr = &c;
    conns.erase(id);
    client_slab.Release(ptr);
}

// =============================================================================
// Outbound path
// =============================================================================

inline void EventLoop::StartConnect(PeerConn& p) {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    if (::getaddrinfo(p.ip, p.port, &hints, &res) != 0 || res == nullptr) {
        return; // peer endpoint unresolvable; leave Disconnected, drop pending
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    FD fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (fd < 0) return;

    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(fd); return; }

    p.fd            = fd;
    p.state         = PeerConn::State::Connecting;
    p.epoll_events  = EPOLLOUT | EPOLLRDHUP;

    try {
        register_fd(fd, p.epoll_events);
    } catch (std::exception&) {
        ::close(fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return;
    }
    peer_fd_to_id[fd] = p.peer_id;
}

inline void EventLoop::OnPeerWritable(PeerConn& p) {
    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return;
        }
        p.state = PeerConn::State::Connected;
        modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP);
        if (p.fd < 0) return; // DropPeer ran inside modify_peer_interest
    }

    while (p.wbuf_offset < p.wbuf.size()) {
        ssize_t n = ::send(p.fd,
                           p.wbuf.data() + p.wbuf_offset,
                           p.wbuf.size() - p.wbuf_offset,
                           MSG_NOSIGNAL);
        if (n > 0) { p.wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        DropPeer(p);
        return;
    }
    p.wbuf.clear();
    p.wbuf_offset = 0;
    modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
}

inline void EventLoop::OnPeerReadable(PeerConn& p) {
    std::byte tmp[RECV_CHUNK];
    for (;;) {
        ssize_t n = ::recv(p.fd, tmp, sizeof(tmp), 0);
        if (n > 0) { p.rbuf.insert(p.rbuf.end(), tmp, tmp + n); continue; }
        if (n == 0) { DropPeer(p); return; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        DropPeer(p);
        return;
    }

    while (!p.inflight.empty() && !p.rbuf.empty()) {
        const PendingReply& head = p.inflight.front();
        size_t consumed = 0;
        try {
            consumed = kReplyEntries[static_cast<size_t>(head.kind)](
                p.rbuf.data(), p.rbuf.size(), head);
        } catch (const std::exception&) {
            DropPeer(p);
            return;
        }
        if (consumed == 0) return; // need more bytes
        p.rbuf.erase(p.rbuf.begin(), p.rbuf.begin() + consumed);
        p.inflight.pop_front();
    }
}

inline void EventLoop::DropPeer(PeerConn& p) {
    if (p.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.fd, nullptr);
        peer_fd_to_id.erase(p.fd);
        ::close(p.fd);
        p.fd = -1;
    }
    p.state        = PeerConn::State::Disconnected;
    p.epoll_events = 0;
    p.wbuf.clear();
    p.wbuf_offset  = 0;
    p.rbuf.clear();
    // In-flight handlers are stranded; their callbacks won't fire. A future
    // pass can either invoke them with a synthetic failure response or
    // implement automatic reconnect + retry.
    p.inflight.clear();
}

// =============================================================================
// Cross-thread inbox
// =============================================================================

inline void EventLoop::OnEventfd() {
    uint64_t counter;
    for (;;) {
        ssize_t n = ::read(event_fd, &counter, sizeof(counter));
        if (n == sizeof(counter)) break;
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        break;
    }
    DrainInbox();
}

inline void EventLoop::DrainInbox() {
    std::vector<Outbound> local;
    {
        std::lock_guard<std::mutex> lk(inbox_mu);
        local.swap(inbox);
    }
    for (auto& out : local) {
        auto it = peer_conns.find(out.peer_id);
        if (it == peer_conns.end()) continue; // not our peer
        PeerConn& p = it->second;

        p.wbuf.insert(p.wbuf.end(), out.bytes.begin(), out.bytes.end());
        p.inflight.push_back(std::move(out.reply));

        if (p.state == PeerConn::State::Disconnected) {
            StartConnect(p);
            // EPOLLOUT already armed; bytes drain after connect completes.
            continue;
        }
        if (p.state == PeerConn::State::Connected) {
            modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        }
        // If Connecting, EPOLLOUT is already armed and OnPeerWritable will
        // drain once the connect lands.
    }
}

inline void EventLoop::EnqueueAE(NodeID peer_id,
                                 AppendEntriesReqPayload payload,
                                 std::function<void(AppendEntriesRespPayload)> on_reply) {
    Outbound out;
    out.peer_id = peer_id;
    serialize_ae_req(payload, out.bytes);
    out.reply.kind  = RpcKind::AppendEntries;
    out.reply.on_ae = std::move(on_reply);
    {
        std::lock_guard<std::mutex> lk(inbox_mu);
        inbox.push_back(std::move(out));
    }
    wake();
}

inline void EventLoop::EnqueueRV(NodeID peer_id,
                                 RequestVoteReqPayload payload,
                                 std::function<void(RequestVoteRespPayload)> on_reply) {
    Outbound out;
    out.peer_id = peer_id;
    serialize_rv_req(payload, out.bytes);
    out.reply.kind  = RpcKind::RequestVote;
    out.reply.on_rv = std::move(on_reply);
    {
        std::lock_guard<std::mutex> lk(inbox_mu);
        inbox.push_back(std::move(out));
    }
    wake();
}

inline void EventLoop::EnqueueIS(NodeID peer_id,
                                 InstallSnapshotReqPayload payload,
                                 std::function<void(InstallSnapshotRespPayload)> on_reply) {
    Outbound out;
    out.peer_id = peer_id;
    serialize_is_req(payload, out.bytes);
    out.reply.kind  = RpcKind::InstallSnapshot;
    out.reply.on_is = std::move(on_reply);
    {
        std::lock_guard<std::mutex> lk(inbox_mu);
        inbox.push_back(std::move(out));
    }
    wake();
}
