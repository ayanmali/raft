#pragma once

#include "./protocol/client.hpp"
#include "conns.hpp"
#include "protocol/payloads.hpp"
#include "../cross_thread.hpp"
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>

constexpr int EPOLL_BATCH = 64; // max # of fds processed per loop iteration
constexpr int MAX_ATTEMPTS = 10;

/*
One event loop runs on one thread.
*/
template <size_t P>
struct EventLoop {
    public:
    EventLoop(FD listen_fd, size_t inbound_cap, NodeReplyInbox<P>& node_reply_inbox_);
    ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Run();
    void Stop(); // event loop can be stopped via a signal on the event fd
    void Wake();

    // void EnqueueAE(NodeID peer_id, AppendEntriesReqPayload req);
    // void EnqueueRV(NodeID peer_id, RequestVoteReqPayload req);
    // void EnqueueIS(NodeID peer_id, InstallSnapshotReqPayload req);
    // void EnqueueArmTimer(NodeID peer_id, std::chrono::nanoseconds period);
    // void EnqueueDisarmTimer(NodeID peer_id);

    using Inbox = MPSC<std::unique_ptr<Outbound>, INBOX_RING_CAP, P>;
    Inbox inbox_{};

    private:
    std::atomic<bool> stopped{false};
    std::atomic<bool> wake_armed{false};

    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd = -1;
    
    // Inbound
    ClientConnSlab client_slab;
    std::unordered_map<ClientID, ClientConn*> client_conns;
    std::unordered_map<FD, ClientID> client_fd_to_id;
    ClientID next_conn_id = 1;

    // Outbound
    NodeReplyInbox<P> node_reply_inbox;
    std::unordered_map<NodeID, PeerConn> peer_conns;
    std::unordered_map<FD, NodeID> peer_fd_to_id;
    std::unordered_map<FD, NodeID> peer_timer_fd_to_id; // for heartbeats
    NodeID next_peer_id = 1;

    // ---- helpers ----
    static void set_nonblocking(FD fd);
    void register_fd(FD fd, uint32_t events);
    void modify_client_interest(ClientConn& c, uint32_t events);
    void modify_peer_interest(PeerConn& p, uint32_t events);
    void post_inflight(AppendEntriesReqPayload& payload, NodeID peer_id);
    void post_inflight(RequestVoteReqPayload& payload, NodeID peer_id);
    void post_inflight(InstallSnapshotReqPayload& payload, NodeID peer_id);

    // inbound messaging
    void Accept();
    void OnClientReadable(ClientConn& c);
    void OnClientWritable(ClientConn& c);
    void CloseClient(ClientConn& c);
    void ReapClient(ClientConn& c);

    // outbound messaging
    
    void OnPeerWritable(PeerConn& p);
    void OnPeerReadable(PeerConn& p);
    void OnPeerTimer(PeerConn& p);
    void StartConnect(PeerConn& p);
    void DropPeer(PeerConn& p);

    // wake / inbox
    void arm_peer_timer(ArmTimerPayload payload, NodeID peer_id);
    void disarm_peer_timer(NodeID peer_id);
    void DrainInbox();
    void OnEventFd();
    void wake_eventfd_unconditional();

    bool post_node_req_inbox(RpcMessage& req);   // to pass incoming RPC requests to the Raft layer
    bool post_node_reply_inbox(RpcReply& reply); // to pass incoming RPC replies to the Raft layer

};

template <size_t P>
inline EventLoop<P>::EventLoop(FD listen_fd, size_t inbound_cap, NodeReplyInbox<P>& node_reply_inbox_) 
: listen_fd(listen_fd), client_slab(inbound_cap), node_reply_inbox(node_reply_inbox_) {
    epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) throw std::runtime_error("epoll_create1 failed");

    event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (event_fd < 0) throw std::runtime_error("eventfd failed");

    set_nonblocking(listen_fd);
    register_fd(listen_fd, EPOLLIN | EPOLLET);
    register_fd(event_fd, EPOLLIN | EPOLLET );

    // TODO: initialize peers in the Node constructor. move this to the EventLoop initialization in Node
    // peer_conns.reserve(peers.size());
    // for (const auto& pe : peers) {
    //     PeerConn pc;
    //     pc.peer_id = pe.id;
    //     pc.ip      = pe.ip;
    //     pc.port    = pe.port;
    //     pc.next_index = 0;
    //     pc.match_index = 0;

    //     pc.timer_fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    //     if (pc.timer_fd < 0) throw std::runtime_error("timerfd_create");
    //     register_fd(pc.timer_fd, EPOLLIN);
    //     peer_timer_to_id[pc.timer_fd] = pe.id;

    //     peer_conns.emplace(pe.id, std::move(pc));

    // }
};


template <size_t P>
inline EventLoop<P>::~EventLoop() {
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }
    // Listen fd is owned by Node.

};

template <size_t P>
inline void EventLoop<P>::Stop() {
    stopped.store(true, std::memory_order_release);
    wake_eventfd_unconditional();
}

template <size_t P>
inline void EventLoop<P>::Wake() {
    if (!wake_armed.exchange(true, std::memory_order_acq_rel)) {
        wake_eventfd_unconditional();
    }
}

template <size_t P>
inline void EventLoop<P>::wake_eventfd_unconditional() {
    uint64_t one = 1;
    ssize_t  n   = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

template <size_t P>
inline void EventLoop<P>::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
    }
}

template <size_t P>
inline void EventLoop<P>::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl ADD failed");
    }
}

template <size_t P>
inline void EventLoop<P>::modify_client_interest(ClientConn& c, uint32_t events) {
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

template <size_t P>
inline void EventLoop<P>::modify_peer_interest(PeerConn& p, uint32_t events) {
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

template <size_t P>
inline void EventLoop<P>::Run() {
    epoll_event evs[EPOLL_BATCH];
    while (!stopped.load(std::memory_order_acquire)) {
        int n = ::epoll_wait(epoll_fd, evs, EPOLL_BATCH, -1);

        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error("epoll_wait failed");
        }

        // loop over all ready FDs
        for (int i = 0; i < n; ++i) {
            const FD fd = evs[i].data.fd;
            const uint32_t e = evs[i].events;

            if (fd == listen_fd) { Accept(); continue; }
            if (fd == event_fd) { OnEventFd(); continue; }

            if (auto it = client_fd_to_id.find(fd); it != client_fd_to_id.end()) {
                ClientConn& c = client_conns.at(it->second);
                if (e & EPOLLERR | EPOLLHUP | EPOLLRDHUP) {
                    CloseClient(c);
                    continue;
                }
                if (e & EPOLLIN) OnClientReadable(c);
                if (e & EPOLLOUT) OnClientWritable(c);
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

            if (auto it = peer_timer_to_id.find(fd); it != peer_timer_fd_to_id.end()) {
                PeerConn& p = peer_conns.at(it->second);
                if (e & EPOLLIN) OnPeerTimer(p);
            }
        }
    }
}

/* Inbound */

template <size_t P>
inline void EventLoop<P>::Accept() {
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
        client_fd_to_id[fd] = c->id;
        client_conns.emplace(c->id, c);
    }
}

template <size_t P>
inline void EventLoop<P>::OnClientWritable(ClientConn& c) {
    while (c.wbuf_offset < c.wbuf.size()) {
        ssize_t n = ::send(
            c.fd,
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
    if (c.closing) ReapClient(c);
}

template <size_t P>
inline void EventLoop<P>::OnClientReadable(ClientConn& c) {
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

    // drain as many complete request frames as the buffer can hold
    while (!c.closing && !c.rbuf.empty()) {
        size_t before = c.rbuf.size();
        
        auto [request, rpc_id] = parse_rbuf(c);
        if (!request) {
            CloseClient(c);
            break;
        }
        RpcMessage& message = request.value();

        // TODO how should this be called?
        std::expected<RpcReply, const char*> response = handle_request<N>(message, rpc_id);
        if (!response) {
            CloseClient(c);
            break;
        }
        RpcReply& reply = response.value();

        c.write_reply(reply);

        if (c.wbuf_offset < c.wbuf.size()) {
            modify_client_interest(c, c.epoll_events | EPOLLOUT);
        }

        if (c.rbuf.size() == before) break; // need more bytes
    }

}

template <size_t P>
inline void EventLoop<P>::CloseClient(ClientConn& c) {
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

template <size_t P>
inline void EventLoop<P>::ReapClient(ClientConn& c) {
    const ClientID id = c.id;
    ClientConn* ptr = &c;
    conns.erase(id);
    client_slab.Release(ptr);
}

/* Outbound */

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
    p.peer_id       = p.peer_id > 0 ? p.peer_id : next_peer_id++; // if this peer is already apart of the configuration, keep its ID the same. If it's new, assign it an ID.  
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
    // TODO: configure per-peer hearbeat timer fds
    // peer_timer_to_id[p.timer_fd] = p.peer_id;
}

template <size_t P>
inline void EventLoop<P>::OnPeerReadable(PeerConn& p) {
    // get last sent RPC
    InflightRPC& out = p.outbox.front();

    // TODO: if too much latency here, replace p.rbuf w a ring buffer, or use readv
    // must drain socket completely b/c of edge-triggered mode
    for (;;) {
        size_t old = out.reply.size();
        out.reply.resize(old + RECV_CHUNK);
        ssize_t n = ::recv(p.fd, out.reply.data() + old, RECV_CHUNK, 0);
        if (n > 0) { out.reply.resize(old + n); continue; }
        if (n == 0) { out.reply.resize(old); DropPeer(p); return; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        DropPeer(p);
        return;
    }

    while (!p.outbox.empty()) {
        const InflightRPC& head = p.outbox.front();
        // TODO; handle the reply at the Raft layer
        std::expected<RpcReply, const char*> reply_raw = parse_reply_buffer(head);
        if (!reply) {
            // TODO: handle error
            break;
        }
        RpcReply& reply = reply_raw.value();
        // TODO how should this be called?
        handle_reply<N>(reply, head.kind);
        post_node_reply_inbox(reply);

        p.outbox.pop_front();
    }

}

template <size_t P>
inline void EventLoop<P>::OnPeerWritable(PeerConn& p) {
    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return;
        }
        p.state = PeerConn::State::Connected;
        modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
    }
    
    InflightRPC& out = p.outbox.front();
    while (out.bytes_sent < out.req.size()) {
        ssize_t n = ::send(p.fd, 
            out.req.data() + out.bytes_sent, 
            out.req.size() - out.bytes_sent,
            MSG_NOSIGNAL);
        if (n > 0) { p.wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        DropPeer(p);
        return;
    }
    out.req.clear();
    out.bytes_sent = 0;
    modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
    // leave this message in the queue for now; its now awaiting a reply
}

template <size_t P>
inline void EventLoop<P>::DropPeer(PeerConn& p) {
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

/* Enqueue functions post a new message to the back of the destination peer struct's outbox queue. */
/*TODO: serialize request bytes into inflightrpc structs*/

// template <size_t P>
// inline void EventLoop<P>::EnqueueAE(NodeID peer_id, AppendEntriesReqPayload payload) {
//     InflightRPC rpc{};
//     rpc.kind = RpcKind::AppendEntries
//     post_inflight(peer_id, rpc);
// }

// template <size_t P>
// inline void EventLoop<P>::EnqueueRV(NodeID peer_id, AppendEntriesReqPayload payload) {
//     InflightRPC rpc{};
//     rpc.kind = RpcKind::RequestVote;
//     post_inflight(peer_id, rpc);
// }

// template <size_t P>
// inline void EventLoop<P>::EnqueueIS(NodeID peer_id, AppendEntriesReqPayload payload) {
//     InflightRPC rpc{};
//     rpc.kind = RpcKind::InstallSnapshot;
//     post_inflight(peer_id, rpc);
// }

// template <size_t P>
// inline void EventLoop<P>::EnqueueArmTimer(NodeID peer_id, std::chrono::nanoseconds period) {
//     InflightRPC rpc;
//     inflight.kind = RpcKind::ArmTimer;
//     post_inflight(peer_id, rpc);
// }

// template <size_t P>
// inline void EventLoop<P>::EnqueueDisarmTimer(NodeID peer_id, std::chrono::nanoseconds period) {
//     InflightRPC rpc;
//     inflight.kind = RpcKind::DisarmTimer;
//     post_inflight(peer_id, rpc);
// }

/* called when draining the messages in the event loop's MPSC inbox. */
template <size_t P>
inline void EventLoop<P>::post_inflight(AppendEntriesReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(out->peer_id);
    if (it == peer_conns.end()) return; // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::AppendEntries, .bytes_sent = 0};
    RPCWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        StartConnect(p);
    }
    else if (p.state == PeerConn:State::Connected) {
        modify_peer_interest(p, p.epoll_events | EPOLLOUT);
    }
}

template <size_t P>
inline void EventLoop<P>::post_inflight(RequestVoteReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(out->peer_id);
    if (it == peer_conns.end()) return; // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::RequestVote, .bytes_sent = 0};
    RPCWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        StartConnect(p);
    }
    else if (p.state == PeerConn:State::Connected) {
        modify_peer_interest(p, p.epoll_events | EPOLLOUT);
    }
}

template <size_t P>
inline void EventLoop<P>::post_inflight(InstallSnapshotReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(out->peer_id);
    if (it == peer_conns.end()) return; // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::InstallSnapshot, .bytes_sent = 0};
    RPCWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        StartConnect(p);
    }
    else if (p.state == PeerConn:State::Connected) {
        modify_peer_interest(p, p.epoll_events | EPOLLOUT);
    }
}

template <size_t N>
inline void EventLoop<N>::arm_peer_timer(ArmTimerPayload payload,
                                         NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;
    PeerConn& p = it->second;

    // Periodic timer: it_value == it_interval == period. The first
    // expiration lands `period` from now; subsequent ones fire at the
    // same cadence until disarmed.
    constexpr long NS_PER_SEC = 1'000'000'000;
    const long ns = static_cast<long>(payload.period.count());
    itimerspec spec{};
    spec.it_value.tv_sec  = ns / NS_PER_SEC;
    spec.it_value.tv_nsec = ns % NS_PER_SEC;
    spec.it_interval      = spec.it_value;

    ::timerfd_settime(p.timer_fd, 0, &spec, nullptr);
}

template <size_t N>
inline void EventLoop<N>::disarm_peer_timer(NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;
    PeerConn& p = it->second;
    // Zero spec disarms; any pending expirations are cleared on the next
    // read. epoll readiness for an already-counted timerfd is harmless --
    // OnPeerTimer just sees expirations==0 and moves on.
    itimerspec zero{};
    ::timerfd_settime(p.timer_fd, 0, &zero, nullptr);
}

/* Cross-thread messaging (e.g. timer logic) */

struct OutboundVisitor {
    public:
    OutboundVisitor(NodeID peer_id) : peer_id{peer_id} {};

    void operator()(const AppendEntriesReqPayload& payload) {
        post_inflight(payload, peer_id);
    }
    void operator()(const RequestVoteReqPayload& payload) {
        post_inflight(payload, peer_id);
    }
    void operator()(const InstallSnapshotReqPayload& payload) {
        post_inflight(payload, peer_id);
    }
    void operator()(ArmTimerPayload payload) {
        arm_peer_timer(payload, peer_id);
    }
    void operator()(DisarmTimerPayload payload) {
        disarm_peer_timer(peer_id);
    }
    private:
    NodeID peer_id;
};

template <size_t N>
inline void EventLoop<N>::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    wake_armed.store(false, std::memory_order_release);

    // inbox_.DrainAll([this](std::unique_ptr<Outbound> out) {
    //     outbound_funcs[static_cast<uint8_t>(out->kind);](std::move(out));
    // });

    inbox.DrainAll([](std::unique_ptr<Outbound> out) {
        std::visit(OutboundVisitor{out->peer_id}, out->data);
    });
    
}

// drains this event loop's MPSC inbox
// for each message in the inbox, enqueue onto the corresponding peer's RPC outbox
template <size_t N>
inline void EventLoop<N>::OnEventFd() {
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

template <size_t P>
inline bool EventLoop<P>::post_node_req_inbox(RpcMessage& req) {};

// to pass incoming RPC replies to the Raft layer
template <size_t P>
inline bool EventLoop<P>::post_node_reply_inbox(RpcReply& reply){
    int counter = 0;
    while (counter < MAX_ATTEMPTS) {
        bool res = node_reply_inbox.Push(reply);
        ++counter;
        if (res) return true;
    }
    return false;
}
