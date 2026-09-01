#pragma once
#include "./event_loop.hpp"
#include "../protocol/utils.hpp"
#include <asm-generic/socket.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <format>
#ifdef DEBUG
#include <iostream>
#endif

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::modify_peer_interest(PeerConn& p, uint32_t events) {
    if (p.epoll_events == events) return {};
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 = (static_cast<uint64_t>(EpollContextKind::Peer) << 56)
                |  static_cast<uint64_t>(p.peer_id);
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, p.fd, &ev) < 0) {
        DropPeer(p);
        return "Error modifying epoll events for peer fd";
    }
    p.epoll_events = events;
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::AddPeer(NodeID id, IPAddrPort ip_addr) {
    auto [addr, port] = decode(ip_addr);
    if (id > peer_id_to_conn.size()) {
        peer_id_to_conn.resize(id + 1);
    }
    auto it = peer_id_to_conn.emplace(peer_id_to_conn.begin() + id, ip_addr, id);
    std::optional<std::string> connect_err = StartConnect(*it);
    if (connect_err) {
        #ifdef DEBUG
        std::cout << "error adding peer " << id << " (ip address = " << ip_addr << ") to configuration: " << connect_err.value() << "\n";
        #endif
        return std::format(
            "Failed to add peer:\n{}\n", connect_err.value()
        );
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::StartConnect(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "attempting to connect to peer " << p.peer_id << "\n";
    #endif
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    if constexpr (SOCKET_TYPE == TCP) {
        hints.ai_socktype = SOCK_STREAM;
    }
    if constexpr (SOCKET_TYPE == UDP) {
        hints.ai_socktype = SOCK_DGRAM;
    }
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    auto [ip_addr, port] = decode(p.peer_ip_addr);
    std::string ip_addr_str = std::to_string(ip_addr);
    std::string port_str = std::to_string(port);

    if (::getaddrinfo(ip_addr_str.c_str(), port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
        return "Error getting address info for peer";
    }

    p.fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (p.fd < 0) return "Failed to start socket";

    int yes = 1;
    if constexpr (SOCKET_TYPE == TCP) {
        ::setsockopt(p.fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    }
    // TODO: tune send/receive buffer size
    if constexpr (SOCKET_TYPE == UDP) {
        const auto send_size = REQ_SIZE + sizeof(REQ_SIZE) + sizeof(RpcKind);
        const auto rcv_size = RESP_SIZE + sizeof(RESP_SIZE) + sizeof(RpcKind);
        ::setsockopt(listen_fd, SOL_SOCKET, SO_SNDBUF, &send_size, sizeof(send_size));
        ::setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &rcv_size, sizeof(rcv_size));
    }

    int rc = ::connect(p.fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(p.fd); return "Failed to connect socket"; }

    p.epoll_events  = EPOLLOUT | EPOLLRDHUP | EPOLLET;
    if constexpr (SOCKET_TYPE == TCP) {
        p.state = PeerConn::State::Connecting;
    }
    if constexpr (SOCKET_TYPE == UDP) {
        p.state = PeerConn::State::Connected;
    }

    int heartbeat_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int ae_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int rv_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int is_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);

    if (heartbeat_fd < 0 || ae_timeout_fd < 0 || rv_timeout_fd < 0 || is_timeout_fd < 0) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to socket for peer {}:\nerror creating timer fds\n",
            p.peer_id
        );
    }

    p.timer_fds.set_heartbeat(heartbeat_fd);
    p.timer_fds.set_ae_timeout(ae_timeout_fd);
    p.timer_fds.set_rv_timeout(rv_timeout_fd);
    p.timer_fds.set_is_timeout(is_timeout_fd);

    std::optional<const char*> peer_fd_err = register_fd(p.fd, p.epoll_events, EpollContextKind::Peer, p.peer_id);
    if (peer_fd_err) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to peer {}:\n{}\n",
            p.peer_id, peer_fd_err.value()
        );
    }

    std::optional<const char*> timer_fd_err = register_fd(heartbeat_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::Heartbeat, p.peer_id);

    std::optional<const char*> ae_timeout_fd_err = register_fd(ae_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::AE, p.peer_id);

    std::optional<const char*> rv_timeout_fd_err = register_fd(rv_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::RV, p.peer_id);

    std::optional<const char*> is_timeout_fd_err = register_fd(is_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::IS, p.peer_id);

    if (timer_fd_err || ae_timeout_fd_err || rv_timeout_fd_err || is_timeout_fd_err) {
        ::close(p.timer_fds.get_heartbeat());
        ::close(p.timer_fds.get_ae_timeout());
        ::close(p.timer_fds.get_rv_timeout());
        ::close(p.timer_fds.get_is_timeout());

        p.timer_fds.set_heartbeat(-1);
        p.timer_fds.set_ae_timeout(-1);
        p.timer_fds.set_rv_timeout(-1);
        p.timer_fds.set_is_timeout(-1);

        p.state = PeerConn::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to peer {}: failed to register timer/timeout fds\n",
            p.peer_id
        );
    }

    #ifdef DEBUG
    std::cout << "finished connecting: peer " << p.peer_id << " socket state = " << static_cast<int>(p.state) << "\n";
    #endif

    ::freeaddrinfo(res);
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerReadable(PeerConn& p) {
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
        return "unexpected error attempting to read from peer socket\n";
    }

    size_t parsed = 0;
    while (end - parsed >= sizeof(uint32_t)) {
        uint32_t net_len;
        std::memcpy(&net_len, p.rbuf + parsed, sizeof(net_len));
        uint32_t msg_len = ntohl(net_len);
        size_t frame_size = msg_len + sizeof(msg_len);
        if (end - parsed < frame_size) break;

        auto result = parse_rbuf(p.rbuf + sizeof(msg_len) + parsed, msg_len, p.timer_fds);
        if (std::holds_alternative<const char*>(result)) break;
        parsed += frame_size;

        #ifdef DEBUG
        std::cout << "passing reply from peer " << p.peer_id << " back to node\n";
        #endif
        post_node_inbox(std::move(std::get<NodeMessage>(result)));
    }
    p.rbuf_offset = end - parsed;
    if (p.rbuf_offset > 0) std::memmove(p.rbuf, p.rbuf + parsed, p.rbuf_offset); // overwrite at the beginning of the buffer
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerWritable(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " writable\n";
    #endif

    if (p.state == PeerConn::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return "error attempting to write to peer socket\n";
        }
        p.state = PeerConn::State::Connected;
        #ifdef DEBUG
        std::cout << "Set peer " << p.peer_id << " fd to Connected\n";
        #endif
        std::optional<const char*> modify_err = modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
        if (modify_err) {
            return modify_err;
        }
    }

    itimerspec spec{};
    const long rpc_timeout_ns = rpc_timeout_ms * 1'000'000; // rpc_timeout is in ms
    spec.it_value.tv_sec  = rpc_timeout_ns / NS_PER_SEC;
    spec.it_value.tv_nsec = rpc_timeout_ns % NS_PER_SEC;
    spec.it_interval      = {0, 0};

    while (p.wbuf_offset < p.wbuf_size) {
        uint8_t kind;
        std::memcpy(&kind, p.wbuf + p.wbuf_offset + sizeof(uint32_t), sizeof(kind));
        if (static_cast<RpcKind>(kind) != RpcKind::ForwardLeader) {
            ::timerfd_settime(p.timer_fds.fds[kind + 1], 0, &spec, nullptr);
        }

        ssize_t n = ::send(p.fd,
            p.wbuf + p.wbuf_offset,
            p.wbuf_size - p.wbuf_offset,
            MSG_NOSIGNAL);
        if (n > 0) { p.wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
        DropPeer(p);
        return "unknown error after writing to peer socket\n";
    }

    // All bytes sent — but only clear if no new data was appended by
    // post_inflight while we were sending (same-thread interleaving via
    // DrainInbox in the same epoll batch).
    if (p.wbuf_offset >= p.wbuf_size) {
        std::memset(p.wbuf, 0, p.wbuf_size);
        p.wbuf_offset = 0;
        p.wbuf_size = 0;
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
        if (modify_err) return modify_err;
    }
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerHeartbeatTimeout(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "heartbeat timer fired for peer " << p.peer_id << "\n";
    #endif
    if (p.timer_fds.get_heartbeat() == -1) return {};
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fds.get_heartbeat(), &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return "error attempting to read peer timer fd\n";
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    post_node_inbox(NodeMessage{HeartbeatTimeout{.source_id = p.peer_id}});
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerAERPCTimeout(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "AE timeout timer fired for peer " << p.peer_id << "\n";
    #endif
    if (p.timer_fds.get_ae_timeout() == -1) return {};
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fds.get_ae_timeout(), &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return "error attempting to read peer AE timeout fd\n";
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    post_node_inbox(NodeMessage{AETimeout{ .source_id = p.peer_id }});
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerRVRPCTimeout(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "RV timeout timer fired for peer " << p.peer_id << "\n";
    #endif
    if (p.timer_fds.get_rv_timeout() == -1) return {};
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fds.get_rv_timeout(), &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return "error attempting to read peer RV timeout fd\n";
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    post_node_inbox(NodeMessage{RVTimeout{ .source_id = p.peer_id }});
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerISRPCTimeout(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "IS timeout timer fired for peer " << p.peer_id << "\n";
    #endif
    if (p.timer_fds.get_is_timeout() == -1) return {};
    uint64_t expirations = 0;
    ssize_t n = ::read(p.timer_fds.get_is_timeout(), &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return "error attempting to read peer IS timeout fd\n";
    }
    if (n != sizeof(expirations) || expirations == 0) return {};

    post_node_inbox(NodeMessage{ISTimeout{ .source_id = p.peer_id }});
    return {};
}

template <SocketType T>
inline void EventLoop<T>::DropPeer(PeerConn& p) {
    #ifdef DEBUG
    std::cout << "Dropping peer " << p.peer_id << "\n";
    #endif
    if (p.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.fd, nullptr);
        ::close(p.fd);
        p.fd = -1;
    }
    if (p.timer_fds.get_heartbeat() >= 0) {
        for (int i = 0; i < 4; ++i) {
            FD tfd = p.timer_fds.fds[i];
            if (tfd >= 0) {
                ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, tfd, nullptr);
                ::close(tfd);
                p.timer_fds.fds[i] = -1;
            }
        }
    }

    p.state        = PeerConn::State::Disconnected;
    p.epoll_events = 0;
    std::memset(p.wbuf, 0, sizeof(p.wbuf));
    p.wbuf_offset = 0;
    p.wbuf_size = 0;
    std::memset(p.rbuf, 0, sizeof(p.rbuf));
    p.rbuf_offset = 0;
    // send message to node thread to remove this peer from its nodes list
    post_node_inbox(NodeMessage{DropPeerMsg{.source_id = p.peer_id}});
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
template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_inflight(AppendEntriesReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting AE RPC to outbound queue for node " << payload.dest_id << "\n";
    std::cout << "payload term = " << payload.term << "\n";
    std::cout << "payload prev log index = " << payload.prev_log_idx << "\n";
    std::cout << "payload prev log term = " << payload.prev_log_term << "\n";
    std::cout << "payload leader commit = " << payload.leader_commit << "\n";
    std::cout << "payload entries_len = " << payload.entries_len << "\n";
    #endif

    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to post AE RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }
    PeerConn& p = peer_id_to_conn[payload.dest_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to post AE RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }

    p.wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(uint8_t);
    #ifdef DEBUG
    std::cout << "p.wbuf_size = " << p.wbuf_size << "\n";
    #endif
    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);
    #ifdef DEBUG
    std::cout << "serialized\n";
    #endif

    if (p.state == PeerConn::State::Connected) {
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (modify_err) {
            return std::format(
                "Failed to post AE RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_err.value()
            );
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_inflight(RequestVoteReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting RV RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif

    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to post RV RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }
    PeerConn& p = peer_id_to_conn[payload.dest_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to post RV RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }

    p.wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(uint8_t);
    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (modify_err) {
            return std::format(
                "Failed to post RV RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_err.value()
            );
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_inflight(InstallSnapshotReqPayload& payload) {
    #ifdef DEBUG
    std::cout << "Posting IS RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif

    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to post IS RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }
    PeerConn& p = peer_id_to_conn[payload.dest_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to post IS RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }

    p.wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(uint8_t);
    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (modify_err) {
            return std::format(
                "Failed to post IS RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_err.value()
            );
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_inflight(ForwardLeaderMsg& payload) {
    #ifdef DEBUG
    std::cout << "Posting FL RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif

    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to post FL RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }
    PeerConn& p = peer_id_to_conn[payload.dest_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to post FL RPC to inflight queue: peer id {} not found in peer_conns\n",
            payload.dest_id);
    }

    p.wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(uint8_t);
    BufByteWriter writer{p.wbuf};
    writer.serialize(payload);

    if (p.state == PeerConn::State::Connected) {
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events | EPOLLOUT);
        if (modify_err) {
            return std::format(
                "Failed to post FL RPC to inflight queue for peer {}:\n{}",
                p.peer_id, modify_err.value()
            );
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::arm_heartbeat_timer(NodeID peer_id) {
    #ifdef DEBUG
    std::cout << "arming heartbeat timer for peer " << peer_id << "\n";
    #endif

    if (peer_id < 0 || peer_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to arm heartbeat timer: peer id {} not found in peer_conns\n",
            peer_id);
    }
    PeerConn& p = peer_id_to_conn[peer_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to arm heartbeat timer: peer id {} not found in peer_conns\n",
            peer_id);
    }

    // Periodic timer: it_value == it_interval == period. The first
    // expiration lands `period` from now; subsequent ones fire at the
    // same cadence until disarmed.
    constexpr long NS_PER_SEC = 1'000'000'000;
    itimerspec spec{};
    const long heartbeat_period_ns = heartbeat_period_ms * 1'000'000; // heartbeat_period is in ms
    spec.it_value.tv_sec  = heartbeat_period_ns / NS_PER_SEC;
    spec.it_value.tv_nsec = heartbeat_period_ns % NS_PER_SEC;
    spec.it_interval      = spec.it_value;
    ::timerfd_settime(p.timer_fds.get_heartbeat(), 0, &spec, nullptr);
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::disarm_heartbeat_timer(NodeID peer_id) {
    #ifdef DEBUG
    std::cout << "disarming heartbeat timer for node " << peer_id << "\n";
    #endif

    if (peer_id < 0 || peer_id >= peer_id_to_conn.size()) {
        return std::format(
            "Failed to disarm heartbeat timer: peer id {} not found in peer_conns\n",
            peer_id);
    }
    PeerConn& p = peer_id_to_conn[peer_id];
    if (p.peer_id == -1) {
        return std::format(
            "Failed to disarm heartbeat timer: peer id {} not found in peer_conns\n",
            peer_id);
    }

    // Zero spec disarms
    // Drain any already-counted expirations so that
    // EPOLLET doesn't deliver a stale read after we return.
    if (p.timer_fds.get_heartbeat() == -1) return {};

    itimerspec zero{};
    ::timerfd_settime(p.timer_fds.get_heartbeat(), 0, &zero, nullptr);
    uint64_t dummy;
    ssize_t n = ::read(p.timer_fds.get_heartbeat(), &dummy, sizeof(dummy));
    (void)n;

    return {};
}
