#include "./event_loop.hpp"
#include <netdb.h>
#include <netinet/tcp.h>
#include "../protocol/peer.hpp"
#include <sys/timerfd.h>
#ifdef DEBUG
#include <iostream>
#endif

VoidExpected EventLoop::modify_peer_interest(PeerConn& p, uint32_t events) {
    if (p.epoll_events == events) return {};
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = p.fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, p.fd, &ev) < 0) {
        DropPeer(p);
        return Unexpected("Error modifying epoll events for peer fd");
    }
    p.epoll_events = events;
    return {};
}

VoidExpectedF EventLoop::AddPeer(const char* ip_addr, const char* port) {
    peer_conns.insert({next_peer_id, PeerConn{ip_addr, port, next_peer_id}});
    PeerConn& p = peer_conns.at(next_peer_id);
    ++next_peer_id;
    VoidExpected connect_ok = StartConnect(p);
    if (!connect_ok) {
        #ifdef DEBUG
        std::cout << "error adding peer to configuration: " << connect_ok.error() << "\n";
        #endif
        return {};
    }
    return {};
}

VoidExpected EventLoop::StartConnect(PeerConn& p) {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    if (::getaddrinfo(p.ip, p.port, &hints, &res) != 0 || res == nullptr) {
        return Unexpected("Error getting address info for peer");
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    p.fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (p.fd < 0) return Unexpected("Failed to start socket");

    int yes = 1;
    ::setsockopt(p.fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    int rc = ::connect(p.fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(p.fd); return Unexpected("Failed to connect socket"); }

    p.state         = PeerConn::State::Connecting;
    p.epoll_events  = EPOLLOUT | EPOLLRDHUP | EPOLLET;
    p.timer_fd      = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);

    VoidExpected peer_fd_ok = register_fd(p.fd, p.epoll_events);
    if (!peer_fd_ok) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return peer_fd_ok;
    }
    peer_fd_to_id[p.fd] = p.peer_id;
    VoidExpected timer_fd_ok = register_fd(p.timer_fd, EPOLLIN | EPOLLET);
    if (!timer_fd_ok) {
        ::close(p.timer_fd);
        p.timer_fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return timer_fd_ok;
    }
    peer_timer_fd_to_id[p.timer_fd] = p.peer_id;
    return {};
}

VoidExpected EventLoop::OnPeerReadable(PeerConn& p) {
    // get last sent RPC
    InflightRPC& out = p.outbox.front();

    // TODO: if too much latency here, replace p.rbuf w a ring buffer, or use readv
    // must drain socket completely b/c of edge-triggered mode
    for (;;) {
        size_t old = out.reply.size();
        out.reply.resize(old + RECV_CHUNK);
        ssize_t n = ::recv(p.fd, out.reply.data() + old, RECV_CHUNK, 0);
        if (n > 0) { out.reply.resize(old + n); continue; }
        if (n == 0) { out.reply.resize(old); return {}; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        DropPeer(p);
        return Unexpected("unexpected error attempting to read from peer socket\n");
    }

    while (!p.outbox.empty()) {
        InflightRPC& head = p.outbox.front();
        std::expected<RpcReply, const char*> reply_raw = parse_reply_buffer(head);
        if (!reply_raw) {
            // TODO: handle error
            return Unexpected(reply_raw.error());
        }
        RpcReply& reply = reply_raw.value();
        post_node_inbox(reply, p.peer_id);

        p.outbox.pop_front();
    }
    return {};
}

VoidExpected EventLoop::OnPeerWritable(PeerConn& p) {
    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return Unexpected("error attempting to write to peer socket\n");
        }
        p.state = PeerConn::State::Connected;
        VoidExpected modify_ok = modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
        if (!modify_ok) {
            return modify_ok;
        }
    }

    InflightRPC& out = p.outbox.front();
    while (out.bytes_sent < out.req.size()) {
        ssize_t n = ::send(p.fd,
            out.req.data() + out.bytes_sent,
            out.req.size() - out.bytes_sent,
            MSG_NOSIGNAL);
        if (n > 0) { out.bytes_sent += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
        DropPeer(p);
        return Unexpected("unknown error after writing to peer socket\n");
    }
    // done sending; clean up
    out.req.clear();
    out.bytes_sent = 0;
    VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
    return modify_ok;
    // leave this message in the queue for now; its now awaiting a reply
}

VoidExpected EventLoop::OnPeerTimer(PeerConn& p) {
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fd, &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return Unexpected("error attempting to read peer timer fd\n");
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    RpcRequest req{HeartbeatTimeoutPayload{}};
    post_node_inbox(req, p.peer_id);
    return {};
}

void EventLoop::DropPeer(PeerConn& p) {
    if (p.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.fd, nullptr);
        peer_fd_to_id.erase(p.fd);
        ::close(p.fd);
        p.fd = -1;
    }
    p.state        = PeerConn::State::Disconnected;
    p.epoll_events = 0;
    p.outbox.clear();
    // TODO: In-flight handlers are stranded; their callbacks won't fire. A future
    // pass can either invoke them with a synthetic failure response or
    // implement automatic reconnect + retry.
    // DLQ?
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
VoidExpectedF EventLoop::post_inflight(AppendEntriesReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(
            std::format("peer id {} not found\n", peer_id)
        );
    } // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::AppendEntries, .bytes_sent = 0};
    ByteWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        VoidExpected connect_ok = StartConnect(p);
        if (!connect_ok) {
            return UnexpectedF(connect_ok.error());
        }
    }
    else if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}

VoidExpectedF EventLoop::post_inflight(RequestVoteReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(
            std::format("peer id {} not found\n", peer_id)
        );
    } // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::RequestVote, .bytes_sent = 0};
    ByteWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        VoidExpected connect_ok = StartConnect(p);
        if (!connect_ok) {
            return UnexpectedF(connect_ok.error());
        }
    }
    else if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}

VoidExpectedF EventLoop::post_inflight(InstallSnapshotReqPayload& payload, NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(
            std::format("peer id {} not found\n", peer_id)
        );
    } // message gets dropped
    PeerConn& p = it->second;

    // serialize bytes into peer outbox
    InflightRPC rpc{.kind = RpcKind::InstallSnapshot, .bytes_sent = 0};
    ByteWriter writer{rpc.req};
    writer.serialize(payload);
    p.outbox.push_back(rpc);

    if (p.state == PeerConn::State::Disconnected) {
        VoidExpected connect_ok = StartConnect(p);
        if (!connect_ok) {
            return UnexpectedF(connect_ok.error());
        }
    }
    else if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}

void EventLoop::arm_peer_timer(NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;
    PeerConn& p = it->second;

    // Periodic timer: it_value == it_interval == period. The first
    // expiration lands `period` from now; subsequent ones fire at the
    // same cadence until disarmed.
    constexpr long NS_PER_SEC = 1'000'000'000;
    const long ns = period;
    itimerspec spec{};
    spec.it_value.tv_sec  = ns / NS_PER_SEC;
    spec.it_value.tv_nsec = ns % NS_PER_SEC;
    spec.it_interval      = spec.it_value;

    ::timerfd_settime(p.timer_fd, 0, &spec, nullptr);
}

void EventLoop::disarm_peer_timer(NodeID peer_id) {
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) return;
    PeerConn& p = it->second;
    // Zero spec disarms; any pending expirations are cleared on the next
    // read. epoll readiness for an already-counted timerfd is harmless --
    // OnPeerTimer just sees expirations==0 and moves on.
    itimerspec zero{};
    ::timerfd_settime(p.timer_fd, 0, &zero, nullptr);
}

VoidExpectedF EventLoop::post_reply(AppendEntriesRespPayload& payload, NodeID client_id) {
    auto it = client_conns.find(client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", client_id)
        );
    } // message gets dropped
    ClientConn* c = it->second;
    //++c.pending_tasks;

    ByteWriter writer{c->wbuf};
    writer.serialize(payload);

    if (c->wbuf_offset < c->wbuf.size()) {
        VoidExpected modify_ok = modify_client_interest(c, c->epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}

VoidExpectedF EventLoop::post_reply(RequestVoteRespPayload& payload, NodeID client_id) {
    auto it = client_conns.find(client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", client_id)
        );
    } // message gets dropped
    ClientConn* c = it->second;
    //++c.pending_tasks;

    ByteWriter writer{c->wbuf};
    writer.serialize(payload);

    if (c->wbuf_offset < c->wbuf.size()) {
        VoidExpected modify_ok = modify_client_interest(c, c->epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}

VoidExpectedF EventLoop::post_reply(InstallSnapshotRespPayload& payload, NodeID client_id) {
    auto it = client_conns.find(client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", client_id)
        );
    } // message gets dropped
    ClientConn* c = it->second;
    //++c.pending_tasks;

    ByteWriter writer{c->wbuf};
    writer.serialize(payload);

    if (c->wbuf_offset < c->wbuf.size()) {
        VoidExpected modify_ok = modify_client_interest(c, c->epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(modify_ok.error());
        }
    }
    return {};
}
