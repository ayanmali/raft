#include "./event_loop.hpp"
#include "../protocol/utils.hpp"
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <format>
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

VoidExpectedF EventLoop::AddPeer(NodeID id, const char* ip_addr, const char* port) {
    peer_conns.insert({id, PeerConn{ip_addr, port, id}});
    PeerConn& p = peer_conns.at(id);
    VoidExpectedF connect_ok = StartConnect(p);
    if (!connect_ok) {
        #ifdef DEBUG
        std::cout << "error adding peer " << id << " (ip address = " << ip_addr << ") to configuration: " << connect_ok.error() << "\n";
        #endif
        return UnexpectedF(std::format(
            "Failed to add peer:\n{}\n", connect_ok.error()
        ));
    }
    return {};
}

VoidExpectedF EventLoop::StartConnect(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "attempting to connect to peer " << p.peer_id << "\n";
    #endif
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    if (::getaddrinfo(p.ip, p.port, &hints, &res) != 0 || res == nullptr) {
        return Unexpected("Error getting address info for peer");
    }

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
    if (int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK); timer_fd >= 0) {
        p.timer_fd  = timer_fd;
    }
    else {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return UnexpectedF(std::format(
            "Failed to connect to socket for peer {}:\nerror creating timer fd\n",
            p.peer_id
        ));
    }

    VoidExpected peer_fd_ok = register_fd(p.fd, p.epoll_events);
    if (!peer_fd_ok) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return UnexpectedF(std::format(
            "Failed to connect to peer {}:\n{}\n",
            p.peer_id, peer_fd_ok.error()
        ));
    }
    peer_fd_to_id[p.fd] = p.peer_id;
    VoidExpected timer_fd_ok = register_fd(p.timer_fd, EPOLLIN | EPOLLET);
    if (!timer_fd_ok) {
        ::close(p.timer_fd);
        p.timer_fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return UnexpectedF(std::format(
            "Failed to connect to peer {}:\n{}\n",
            p.peer_id, timer_fd_ok.error()
        ));
    }
    peer_timer_fd_to_id[p.timer_fd] = p.peer_id;
    #ifdef DEBUG
    std::cout << "finished connecting: peer " << p.peer_id << " socket state = " << static_cast<int>(p.state) << "\n";
    #endif

    ::freeaddrinfo(res);
    return {};
}

VoidExpected EventLoop::OnPeerReadable(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " readable\n";
    #endif

    size_t end = p.rbuf_offset;
    for (;;) {
        ssize_t n = ::recv(p.fd, p.rbuf + end, sizeof(p.rbuf) - end, 0);
        if (n > 0) { end += n; continue; }
        if (n == 0) { return {}; }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }
        DropPeer(p);
        return Unexpected("unexpected error attempting to read from peer socket\n");
    }

    size_t parsed = 0;
    while (end - parsed >= sizeof(uint32_t)) {
        uint32_t net_len;
        std::memcpy(&net_len, p.rbuf + parsed, sizeof(net_len));
        uint32_t msg_len = ntohl(net_len);
        size_t frame_size = msg_len + sizeof(msg_len);
        if (end - parsed < frame_size) break;

        auto result = parse_rbuf(p.rbuf + sizeof(msg_len) + parsed, msg_len);
        if (!result) break;
        parsed += frame_size;

        #ifdef DEBUG
        std::cout << "passing reply from peer " << p.peer_id << " back to node\n";
        #endif
        post_node_inbox(std::move(*result));
    }
    p.rbuf_offset = end - parsed;
    if (p.rbuf_offset > 0) std::memmove(p.rbuf, p.rbuf + parsed, p.rbuf_offset); // overwrite at the beginning of the buffer
    return {};
}

VoidExpected EventLoop::OnPeerWritable(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " writable\n";
    #endif

    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return Unexpected("error attempting to write to peer socket\n");
        }
        p.state = PeerConn::State::Connected;
        #ifdef DEBUG
        std::cout << "Set peer " << p.peer_id << " fd to Connected\n";
        #endif
        VoidExpected modify_ok = modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
        if (!modify_ok) {
            return modify_ok;
        }
    }

    while (p.wbuf_offset < sizeof(p.wbuf)) {
        ssize_t n = ::send(p.fd,
            p.wbuf + p.wbuf_offset,
            sizeof(p.wbuf) - p.wbuf_offset,
            MSG_NOSIGNAL);
        if (n > 0) { p.wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
        DropPeer(p);
        return Unexpected("unknown error after writing to peer socket\n");
    }

    // All bytes sent — but only clear if no new data was appended by
    // post_inflight while we were sending (same-thread interleaving via
    // DrainInbox in the same epoll batch).
    if (p.wbuf_offset >= sizeof(p.wbuf)) {
        std::memset(p.wbuf, 0, sizeof(p.wbuf));
        p.wbuf_offset = 0;
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
        if (!modify_ok) return modify_ok;
    }
    return {};
}

VoidExpected EventLoop::OnPeerTimer(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "heartbeat timer fired for peer " << p.peer_id << "\n";
    #endif
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fd, &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return Unexpected("error attempting to read peer timer fd\n");
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    post_node_inbox(RpcMessage{HeartbeatTimeout{.source_id = p.peer_id}});
    return {};
}


void EventLoop::DropPeer(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "Dropping peer " << p.peer_id << "\n";
    #endif
    if (p.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.fd, nullptr);
        peer_fd_to_id.erase(p.fd);
        ::close(p.fd);
    }
    if (p.timer_fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.timer_fd, nullptr);
        peer_timer_fd_to_id.erase(p.timer_fd);
        ::close(p.timer_fd);
    }
    p.state        = PeerConn::State::Disconnected;
    p.epoll_events = 0;
    std::memset(p.wbuf, 0, sizeof(p.wbuf));
    p.wbuf_offset = 0;
    std::memset(p.rbuf, 0, sizeof(p.rbuf));
    peer_conns.erase(p.peer_id);
    // send message to node thread to remove this peer from its nodes list
    post_node_inbox(RpcMessage{DropPeerMsg{.source_id = p.peer_id}});
}

/* Enqueue functions post a new message to the back of the destination peer struct's outbox queue. */

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
VoidExpectedF EventLoop::post_inflight(AppendEntriesReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting AE RPC to outbound queue for node " << payload.dest_id << "\n";
    std::cout << "payload term = " << payload.term << "\n";
    std::cout << "payload prev log index = " << payload.prev_log_idx << "\n";
    std::cout << "payload prev log term = " << payload.prev_log_term << "\n";
    std::cout << "payload leader commit = " << payload.leader_commit << "\n";
    #endif
    auto it = peer_conns.find(payload.dest_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(std::format(
            "Failed to post AE RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id
        ));
    }
    PeerConn& p = it->second;

    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(std::format(
                "Failed to post AE RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_ok.error()
            ));
        }
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::post_inflight(RequestVoteReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting RV RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    auto it = peer_conns.find(payload.dest_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(std::format(
            "Failed to post RV RPC to inflight queue: peer id {} not found in peer_conns\n", payload.dest_id
        ));
    }
    PeerConn& p = it->second;

    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(std::format(
                "Failed to post RV RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_ok.error()
            ));
        }
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::post_inflight(InstallSnapshotReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting IS RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    auto it = peer_conns.find(payload.dest_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(
            std::format("Failed to post IS RPC to inflight queue: peer id {} not found in peer_conns\n", payload.dest_id)
        );
    }
    PeerConn& p = it->second;

    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(std::format(
                "Failed to post IS RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_ok.error()
            ));
        }
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::post_inflight(ForwardLeaderMsg& payload) {
    #ifdef DEBUG
    std::cout << "Posting FL RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    auto it = peer_conns.find(payload.dest_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(
            std::format("Failed to post FL RPC to inflight queue: peer id {} not found in peer_conns\n", payload.dest_id)
        );
    }
    PeerConn& p = it->second;

    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        VoidExpected modify_ok = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (!modify_ok) {
            return UnexpectedF(std::format(
                "Failed to post FL RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_ok.error()
            ));
        }
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::arm_heartbeat_timer(NodeID peer_id) {
    #ifdef DEBUG
    std::cout << "arming heartbeat timer for peer " << peer_id << "\n";
    #endif
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end() || it->second.timer_fd < 0) {
        return UnexpectedF(std::format(
            "Failed to arm heartbeat timer for peer {}; peer id not found in peer_conns or peer timer_fd < 0\n",
            peer_id
        ));
    }
    PeerConn& p = it->second;

    // Periodic timer: it_value == it_interval == period. The first
    // expiration lands `period` from now; subsequent ones fire at the
    // same cadence until disarmed.
    constexpr long NS_PER_SEC = 1'000'000'000;
    const long ns = heartbeat_period;
    itimerspec spec{};
    spec.it_value.tv_sec  = ns / NS_PER_SEC;
    spec.it_value.tv_nsec = ns % NS_PER_SEC;
    spec.it_interval      = spec.it_value;

    ::timerfd_settime(p.timer_fd, 0, &spec, nullptr);
    return {};
}

VoidExpectedF EventLoop::disarm_heartbeat_timer(NodeID peer_id) {
    #ifdef DEBUG
    std::cout << "disarming heartbeat timer for node " << peer_id << "\n";
    #endif
    auto it = peer_conns.find(peer_id);
    if (it == peer_conns.end()) {
        return UnexpectedF(std::format(
            "Failed to disarm heartbeat timer for peer {}; peer id not found in peer_conns\n",
            peer_id
        ));
    }
    if (it->second.timer_fd < 0) {
        return UnexpectedF(std::format(
            "Failed to disarm heartbeat timer for peer {}; peer timer_fd < 0\n",
            peer_id
        ));
    }
    PeerConn& p = it->second;
    // Zero spec disarms; any pending expirations are cleared on the next
    // read. epoll readiness for an already-counted timerfd is harmless --
    // OnPeerTimer just sees expirations==0 and moves on.
    itimerspec zero{};
    ::timerfd_settime(p.timer_fd, 0, &zero, nullptr);
    return {};
}
