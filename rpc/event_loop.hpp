#pragma once
/*
Per-thread EventLoop.

A Node<N> spawns N worker threads, each running its own EventLoop<N>.
Each loop owns:
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
  Node calls EnqueueAE/RV/IS on the *target* loop (the one whose
  peer_id % N matches). The producer thread heap-allocates an
  Outbound, serializes the request into it, and pushes the pointer
  into the target loop's lock-free MPSC inbox at the slot indexed by
  the producer's loop id. A wake-coalesced eventfd write nudges the
  target loop. The consumer drains all sub-rings round-robin in
  OnEventfd, lazily kicks a non-blocking connect for any peer in
  Disconnected, appends bytes to peer.wbuf, pushes the PendingReply
  onto peer.inflight, arms EPOLLOUT, then `delete`s the Outbound.
  Connection completion arrives as EPOLLOUT in Connecting; SO_ERROR
  is checked. Once Connected, OnPeerWritable drains; OnPeerReadable
  calls try_parse_*_resp in inflight-FIFO order and fires the
  corresponding PendingReply callback on the loop thread.
*/

#include "./conns.hpp"
#include "./payloads.hpp"
#include "./protocol.hpp"
#include "../queues/mpsc.hpp"
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

inline constexpr int    EPOLL_BATCH      = 64; // max # of fds processed per loop iteration
inline constexpr size_t RECV_CHUNK       = 4096;
inline constexpr size_t INBOX_RING_CAP   = 64;  // per producer; must be power of 2

// Each event-loop thread sets this to its loop index at the top of Run().
// EnqueueAE/RV/IS read it to pick the producer's MPSC sub-queue slot. Any
// thread that calls into Node::send_*_rpc must therefore be an event-loop
// thread (the whole design assumes Raft state machine work runs on loops).
inline thread_local size_t g_loop_producer_id = SIZE_MAX;

// RpcHandlers and the per-direction dispatch tables (kRpcEntries,
// kReplyEntries) live in protocol.hpp so they can be reused by other
// callers and so the id-to-action mapping has a single source of truth.

template <size_t N>
struct EventLoop {
    static_assert(N > 0 && (N & (N - 1)) == 0,
                  "EventLoop<N>: N must be a power of 2 (MPSC requirement)");

    template <std::ranges::input_range R>
    EventLoop(FD listen_fd,
              R&& peers,
              RpcHandlers handlers,
              size_t inbound_cap,
              size_t my_id);
    ~EventLoop();

    EventLoop(const EventLoop&)            = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Run();
    void Stop();

    // Populated by Node post-construction (two-phase init): all sibling
    // EventLoops, indexed by their my_id_. Enqueue* uses this to route
    // outbound RPCs to the loop that owns the destination peer.
    void set_loops(std::array<EventLoop<N>*, N>& loops);

    // Wake-coalesced. Safe to call from any thread. Used by sibling loops
    // after they push into this loop's inbox, and by Stop() (indirectly
    // via the unconditional wake) to break out of epoll_wait.
    void Wake();

    // Cross-thread RPC entry points. Computes target = peer_id % N and
    // pushes into `loops_[target]`'s inbox at slot g_loop_producer_id, so
    // Node can call any loop's Enqueue* (including the caller's own) and
    // routing happens internally. The calling thread must be an
    // event-loop thread (i.e. g_loop_producer_id < N).
    void EnqueueAE(NodeID peer_id,
                   AppendEntriesReqPayload payload,
                   std::function<void(AppendEntriesRespPayload)> on_reply);
    void EnqueueRV(NodeID peer_id,
                   RequestVoteReqPayload payload,
                   std::function<void(RequestVoteRespPayload)> on_reply);
    void EnqueueIS(NodeID peer_id,
                   InstallSnapshotReqPayload payload,
                   std::function<void(InstallSnapshotRespPayload)> on_reply);

    // Cross-thread timer control. Same routing topology as Enqueue*: the
    // call lands on whatever loop, computes target = peer_id % N, and
    // pushes a tagged Outbound into the target loop's inbox so the target
    // (and only the target) issues the timerfd_settime syscall.
    //
    // Use these only for *cross-loop* arms (e.g. Node-side leader-elected
    // hooks). For same-loop re-arming inside a reply callback, call the
    // private arm_peer_timer helper directly to avoid pointless heap
    // traffic and a round-trip through the inbox.
    void EnqueueArmTimer(NodeID peer_id, std::chrono::nanoseconds period);
    void EnqueueDisarmTimer(NodeID peer_id);

private:
    // Outbound is heap-allocated by the producer and freed by the consumer
    // after its state has been moved into peer.wbuf and peer.inflight (or
    // the timer arm/disarm has been issued). Only raw `Outbound*`
    // (trivially copyable) travels through the SPSC ring.
    enum class Kind : uint8_t { Rpc, ArmTimer, DisarmTimer };
    struct Outbound {
        // Kind::Rpc body. Default-constructed (and harmless) for control
        // kinds; the ~80 B of empty std::vector + std::function slots is
        // negligible until profiling says otherwise.
        std::vector<std::byte> bytes;
        PendingReply           reply;

        // Kind::ArmTimer body. Ignored by other kinds.
        std::chrono::nanoseconds period{0};

        // Selects which body of this struct is meaningful. Drives the
        // dispatch switch in DrainInbox.
        Kind kind = Kind::Rpc;

        // Routing key (target loop = peer_id % N). Always meaningful.
        NodeID peer_id = 0;
    };

    using Inbox = MPSC<Outbound*, INBOX_RING_CAP, N>;

    // ---- fds and lifecycle -----
    FD epoll_fd  = -1;
    FD listen_fd = -1;
    FD event_fd  = -1;
    std::atomic<bool> stopped{false};

    // Wake-coalescing flag. true == a wake has been signaled by some
    // producer and the consumer has not yet drained. Producers test-and-set
    // (false -> true) before issuing eventfd_write; only the winning
    // producer pays the syscall. The consumer disarms (stores false) at
    // the top of DrainInbox -- BEFORE draining the inbox -- so any
    // producer that pushes during the drain sees armed=false, rearms, and
    // re-wakes. This guarantees no lost items.
    std::atomic<bool> wake_armed{false};

    size_t       my_id_;
    RpcHandlers  handlers;

    // Sibling pointers populated post-construction by Node::start(). All
    // entries are non-null after set_loops() and stay valid for the
    // lifetime of the Node. Read-only on the loop threads after init.
    std::array<EventLoop<N>*, N> loops_{};

    // ---- inbound ----
    ClientConnSlab                            client_slab;
    std::unordered_map<ClientID, ClientConn*> conns;
    std::unordered_map<FD, ClientID>          fd_to_id;
    ClientID next_conn_id = 1;

    // ---- outbound ----
    std::unordered_map<NodeID, PeerConn> peer_conns;
    std::unordered_map<FD, NodeID>       peer_fd_to_id;
    std::unordered_map<FD, NodeID>       peer_timer_to_id; // for heartbeats

    // ---- cross-thread inbox (lock-free) ----
    Inbox inbox_;

    // ---- helpers ----
    static void set_nonblocking(FD fd);
    void register_fd(FD fd, uint32_t events);
    void modify_client_interest(ClientConn& c, uint32_t events);
    void modify_peer_interest(PeerConn& p, uint32_t events);

    // Unconditional eventfd_write -- bypasses wake_armed. Used by Stop()
    // so shutdown wakes the loop even if nobody else is producing.
    void wake_eventfd_unconditional();

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

    // Per-Outbound dispatch helpers, all called on the owning loop's thread
    // from inside DrainInbox. handle_rpc_outbound runs the existing
    // wbuf-append / inflight-push / EPOLLOUT-arm sequence; arm/disarm wrap
    // timerfd_settime. The same-loop variants of arm/disarm are also safe
    // to call directly from reply callbacks (which already run here),
    // avoiding a round-trip through the inbox.
    void handle_rpc_outbound(Outbound& out);
    void arm_peer_timer(NodeID peer_id, std::chrono::nanoseconds period);
    void disarm_peer_timer(NodeID peer_id);

    // shared by EnqueueAE/RV/IS/ArmTimer/DisarmTimer once the Outbound is
    // fully built: route to loops_[peer_id % N], push, wake-coalesce.
    void post_outbound(Outbound* out);
};

// =============================================================================
// Implementation
// =============================================================================

template <size_t N, std::ranges::input_range R>
inline EventLoop<N>::EventLoop(FD listen_fd_,
                               R&& peers,
                               RpcHandlers handlers_,
                               size_t inbound_cap,
                               size_t my_id)
    : listen_fd(listen_fd_),
      my_id_(my_id),
      handlers(std::move(handlers_)),
      client_slab(inbound_cap) {
    epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) throw std::runtime_error("epoll_create1 failed");

    event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd < 0) throw std::runtime_error("eventfd failed");

    set_nonblocking(listen_fd);
    register_fd(listen_fd, EPOLLIN | EPOLLET);
    register_fd(event_fd,  EPOLLIN);

    peer_conns.reserve(peers.size());
    for (const auto& pe : peers) {
        PeerConn pc;
        pc.peer_id = pe.id;
        pc.ip      = pe.ip;
        pc.port    = pe.port;
        pc.next_index = 0;
        pc.match_index = 0;

        pc.timer_fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (pc.timer_fd < 0) throw std::runtime_error("timerfd_create");
        register_fd(pc.timer_fd, EPOLLIN);
        peer_timer_to_id[pc.timer_fd] = pe.id;

        peer_conns.emplace(pe.id, std::move(pc));

    }

}

template <size_t N>
inline EventLoop<N>::~EventLoop() {
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }
    // Listen fd is owned by Node.

    // Drain anything still in the inbox so we don't leak the Outbound
    // payloads (their on_reply std::functions hold captured state).
    inbox_.DrainAll([](Outbound* out) { delete out; });
}

template <size_t N>
inline void EventLoop<N>::Stop() {
    stopped.store(true, std::memory_order_release);
    wake_eventfd_unconditional();
}

template <size_t N>
inline void EventLoop<N>::Wake() {
    if (!wake_armed.exchange(true, std::memory_order_acq_rel)) {
        wake_eventfd_unconditional();
    }
}

template <size_t N>
inline void EventLoop<N>::wake_eventfd_unconditional() {
    uint64_t one = 1;
    ssize_t  n   = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

template <size_t N>
inline void EventLoop<N>::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
    }
}

template <size_t N>
inline void EventLoop<N>::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl ADD failed");
    }
}

template <size_t N>
inline void EventLoop<N>::modify_client_interest(ClientConn& c, uint32_t events) {
    if (c.epoll_events == events) return;
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = c.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c.fd, &ev) < 0) {
        CloseClient(c);
        throw std::runtime_error("Error modifying events for client fd");
    }
    c.epoll_events = events;
}

template <size_t N>
inline void EventLoop<N>::modify_peer_interest(PeerConn& p, uint32_t events) {
    if (p.epoll_events == events) return;
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = p.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, p.fd, &ev) < 0) {
        DropPeer(p);
        throw std::runtime_error("Error modifying epoll events for peer fd");
    }
    p.epoll_events = events;
}

// ---- main run loop ----------------------------------------------------------

template <size_t N>
inline void EventLoop<N>::Run() {
    g_loop_producer_id = my_id_;

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
                if (e & EPOLLIN)  OnPeerReadable(p);
                if (e & EPOLLOUT) OnPeerWritable(p);
                continue;
            }

            if (auto it = peer_timer_to_id.find(fd); it != peer_timer_to_id.end()) {
                PeerConn& p = peer_conns.at(it->second);
                if (e & EPOLLIN) OnPeerTimer(p);
                continue;
            }
        }
    }
}

// =============================================================================
// Inbound path
// =============================================================================

template <size_t N>
inline void EventLoop<N>::Accept() {
    for (;;) {
        ClientConn* c = client_slab.Acquire();
        if (!c) continue;
            
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

        c->fd = fd;
        c->id = next_conn_id++;
        c->epoll_events = EPOLLIN | EPOLLRDHUP | EPOLLET;

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

template <size_t N>
inline void EventLoop<N>::OnReadable(ClientConn& c) {
    // obtaining the raw bytes from the wire
    // can possibly yield incomplete message frames
    // TODO: if latency is too high here, replace c.rbuf w a ring buffer, or use readv
    for (;;) {
        size_t old = c.rbuf.size();
        c.rbuf.resize(old + RECV_CHUNK);
        ssize_t n = ::recv(c.fd, c.rbuf.data() + old, RECV_CHUNK, 0);
        if (n > 0) { c.rbuf.resize(old + n); continue; }
        if (n == 0) { c.rbuf.resize(old); CloseClient(c); return; }
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

template <size_t N>
inline void EventLoop<N>::DispatchOneRequest(ClientConn& c) {
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

template <size_t N>
inline void EventLoop<N>::OnWritable(ClientConn& c) {
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

template <size_t N>
inline void EventLoop<N>::CloseClient(ClientConn& c) {
    if (c.closing) return;
    c.closing = true;

    if (c.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c.fd, nullptr);
        ::close(c.fd);
        fd_to_id.erase(c.fd);
        c.fd = -1;
    }
    c.epoll_events = 0;

    if (c.pending_tasks == 0) ReapClient(c);
}

template <size_t N>
inline void EventLoop<N>::ReapClient(ClientConn& c) {
    const ClientID id = c.id;
    ClientConn* ptr = &c;
    conns.erase(id);
    client_slab.Release(ptr);
}

// =============================================================================
// Outbound path
// =============================================================================

template <size_t N>
inline void EventLoop<N>::StartConnect(PeerConn& p) {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    if (::getaddrinfo(p.ip, p.port, &hints, &res) != 0 || res == nullptr) {
        throw std::runtime_error("Error getting address info for peer")
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    FD fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (fd < 0) throw std::runtime_error("Failed to start socket");

    int yes = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    int rc = ::connect(fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(fd); throw std::runtime_error("Failed to connect socket"); }

    p.fd            = fd;
    p.state         = PeerConn::State::Connecting;
    p.epoll_events  = EPOLLOUT | EPOLLRDHUP | EPOLLET;

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
    // peer_timer_to_id[p.timer_fd] = p.peer_id;
}

template <size_t N>
inline void EventLoop<N>::OnPeerWritable(PeerConn& p) {
    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return;
        }
        p.state = PeerConn::State::Connected;
        modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
        // if (p.fd < 0) return; // DropPeer ran inside modify_peer_interest
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

template <size_t N>
inline void EventLoop<N>::OnPeerReadable(PeerConn& p) {
    // TODO: if too much latency here, replace p.rbuf w a ring buffer, or use readv
    for (;;) {
        size_t old = p.rbuf.size();
        p.rbuf.resize(old + RECV_CHUNK);
        ssize_t n = ::recv(p.fd, p.rbuf.data() + old, RECV_CHUNK, 0);
        if (n > 0) { p.rbuf.resize(old + n); continue; }
        if (n == 0) { p.rbuf.resize(old); DropPeer(p); return; }
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

template <size_t N>
inline void EventLoop<N>::DropPeer(PeerConn& p) {
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

template <size_t N>
inline void EventLoop<N>::OnEventfd() {
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

template <size_t N>
inline void EventLoop<N>::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    wake_armed.store(false, std::memory_order_release);

    inbox_.DrainAll([this](Outbound* out) {
        switch (out->kind) {
            case Kind::Rpc:
                handle_rpc_outbound(*out);
                break;
            case Kind::ArmTimer:
                arm_peer_timer(out->peer_id, out->period);
                break;
            case Kind::DisarmTimer:
                disarm_peer_timer(out->peer_id);
                break;
        }
        delete out;
    });
}

template <size_t N>
inline void EventLoop<N>::handle_rpc_outbound(Outbound& out) {
    auto it = peer_conns.find(out.peer_id);
    if (it == peer_conns.end()) {
        // Peer not owned by this loop. Producer routing is wrong; drop
        // the request rather than crash. Outbound is freed by caller.
        return;
    }
    PeerConn& p = it->second;

    p.wbuf.insert(p.wbuf.end(), out.bytes.begin(), out.bytes.end());
    p.inflight.push_back(std::move(out.reply));

    if (p.state == PeerConn::State::Disconnected) {
        StartConnect(p);
        // EPOLLOUT already armed; bytes drain after connect completes.
    } else if (p.state == PeerConn::State::Connected) {
        modify_peer_interest(p, p.epoll_events | EPOLLOUT);
    }
    // If Connecting, EPOLLOUT is already armed and OnPeerWritable will
    // drain once the connect lands.
}

template <size_t N>
inline void EventLoop<N>::arm_peer_timer(NodeID peer_id,
                                         std::chrono::nanoseconds period) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;

    // Periodic timer: it_value == it_interval == period. The first
    // expiration lands `period` from now; subsequent ones fire at the
    // same cadence until disarmed.
    constexpr long NS_PER_SEC = 1'000'000'000;
    const long ns = static_cast<long>(period.count());
    itimerspec spec{};
    spec.it_value.tv_sec  = ns / NS_PER_SEC;
    spec.it_value.tv_nsec = ns % NS_PER_SEC;
    spec.it_interval      = spec.it_value;

    ::timerfd_settime(it->second.timer_fd, 0, &spec, nullptr);
}

template <size_t N>
inline void EventLoop<N>::disarm_peer_timer(NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;
    // Zero spec disarms; any pending expirations are cleared on the next
    // read. epoll readiness for an already-counted timerfd is harmless --
    // OnPeerTimer just sees expirations==0 and moves on.
    itimerspec zero{};
    ::timerfd_settime(it->second.timer_fd, 0, &zero, nullptr);
}

template <size_t N>
inline void EventLoop<N>::set_loops(std::array<EventLoop<N>*, N>& loops) {
    loops_ = loops;
}

template <size_t N>
inline void EventLoop<N>::post_outbound(Outbound* out) {
    // The caller must be an event-loop thread; otherwise we'd index off
    // the end of the MPSC sub-queue array. Non-loop callers (Stop, dtors)
    // never enqueue, so this assert defends against future misuse.
    assert(g_loop_producer_id < N && "Enqueue* called from non-event-loop thread");

    const size_t target = static_cast<size_t>(out->peer_id) % N;
    EventLoop<N>* t = loops_[target];
    assert(t != nullptr && "EventLoop::set_loops not called");

    if (!t->inbox_.Push(g_loop_producer_id, out)) {
        // Ring full. With INBOX_RING_CAP=64 and Raft's per-peer in-flight
        // bound of 1, this should be unreachable under normal operation.
        // Treat as fatal so an operator hears about it; TODO: future passes can
        // synthesize a failure reply instead.
        delete out;
        assert(false && "EventLoop inbox ring full");
        return;
    }
    t->Wake();
}

template <size_t N>
inline void EventLoop<N>::EnqueueAE(NodeID peer_id,
                                    AppendEntriesReqPayload payload,
                                    std::function<void(AppendEntriesRespPayload)> on_reply) {
    auto* out = new Outbound{};
    out->peer_id = peer_id;
    serialize_ae_req(payload, out->bytes);
    out->reply.kind  = RpcKind::AppendEntries;
    out->reply.on_ae = std::move(on_reply);
    post_outbound(out);
}

template <size_t N>
inline void EventLoop<N>::EnqueueRV(NodeID peer_id,
                                    RequestVoteReqPayload payload,
                                    std::function<void(RequestVoteRespPayload)> on_reply) {
    auto* out = new Outbound{};
    out->peer_id = peer_id;
    serialize_rv_req(payload, out->bytes);
    out->reply.kind  = RpcKind::RequestVote;
    out->reply.on_rv = std::move(on_reply);
    post_outbound(out);
}

template <size_t N>
inline void EventLoop<N>::EnqueueIS(NodeID peer_id,
                                    InstallSnapshotReqPayload payload,
                                    std::function<void(InstallSnapshotRespPayload)> on_reply) {
    auto* out = new Outbound{};
    out->peer_id = peer_id;
    serialize_is_req(payload, out->bytes);
    out->reply.kind  = RpcKind::InstallSnapshot;
    out->reply.on_is = std::move(on_reply);
    post_outbound(out);
}

template <size_t N>
inline void EventLoop<N>::EnqueueArmTimer(NodeID peer_id,
                                          std::chrono::nanoseconds period) {
    auto* out = new Outbound{};
    out->kind    = Kind::ArmTimer;
    out->peer_id = peer_id;
    out->period  = period;
    post_outbound(out);
}

template <size_t N>
inline void EventLoop<N>::EnqueueDisarmTimer(NodeID peer_id) {
    auto* out = new Outbound{};
    out->kind    = Kind::DisarmTimer;
    out->peer_id = peer_id;
    post_outbound(out);
}

void OnPeerTimer(PeerConn& p) {
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fd, &expirations, sizeof(expirations));
    (void)n; // EAGAIN means it was cleared by another wake; ignore.
    // Hand off to Node. We're already on the owning loop, so this can
    // synchronously call back into EnqueueAE for this peer.
    handlers.on_peer_tick(p.peer_id);
}
