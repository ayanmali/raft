#pragma once
#include "./event_loop.hpp"
#include "../protocol/utils.hpp"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <format>
#ifdef DEBUG
#include <iostream>
#endif

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::modify_peer_interest(PeerConn<T>& p, uint32_t events) {
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
    if (id >= peer_id_to_conn.size()) {
        peer_id_to_conn.resize(id + 1);
    }
    PeerConn<T>& p = peer_id_to_conn[id];
    p.peer_ip_addr = ip_addr;
    p.peer_id = id;
    std::optional<std::string> connect_err = StartConnect(p);
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
template <>
inline std::optional<std::string> EventLoop<TCP>::StartConnect(PeerConn<TCP>& p) {
    #ifdef DEBUG
    std::cout << "attempting to connect to peer " << p.peer_id << "\n";
    #endif
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    auto [ip_addr, port] = decode(p.peer_ip_addr); // stored in network byte order
    struct in_addr addr{ .s_addr = ip_addr };
    char ip_addr_str[INET_ADDRSTRLEN];
    if (::inet_ntop(AF_INET, &addr, ip_addr_str, sizeof(ip_addr_str)) == nullptr) {
        return "Error converting peer IP address to string";
    }
    std::string port_str = std::to_string(ntohs(port));

    if (::getaddrinfo(ip_addr_str, port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
        return "Error getting address info for peer";
    }

    p.fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (p.fd < 0) return "Failed to start socket";

    int yes = 1;
    ::setsockopt(p.fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    int rc = ::connect(p.fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(p.fd); return "Failed to connect socket"; }

    p.epoll_events  = EPOLLOUT | EPOLLRDHUP | EPOLLET;
    p.state = PeerConn<TCP>::State::Connecting;

    int ae_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int rv_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int is_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);

    if (ae_timeout_fd < 0 || rv_timeout_fd < 0 || is_timeout_fd < 0) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn<TCP>::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to socket for peer {}:\nerror creating timer fds\n",
            p.peer_id
        );
    }

    p.timer_fds.set_ae_timeout(ae_timeout_fd);
    p.timer_fds.set_rv_timeout(rv_timeout_fd);
    p.timer_fds.set_is_timeout(is_timeout_fd);

    std::optional<const char*> peer_fd_err = register_fd(p.fd, p.epoll_events, EpollContextKind::Peer, p.peer_id);
    if (peer_fd_err) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn<TCP>::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to peer {}:\n{}\n",
            p.peer_id, peer_fd_err.value()
        );
    }

    std::optional<const char*> ae_timeout_fd_err = register_fd(ae_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::AE, p.peer_id);
    std::optional<const char*> rv_timeout_fd_err = register_fd(rv_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::RV, p.peer_id);
    std::optional<const char*> is_timeout_fd_err = register_fd(is_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::IS, p.peer_id);

    if (ae_timeout_fd_err || rv_timeout_fd_err || is_timeout_fd_err) {
        ::close(p.timer_fds.get_ae_timeout());
        ::close(p.timer_fds.get_rv_timeout());
        ::close(p.timer_fds.get_is_timeout());

        p.timer_fds.set_ae_timeout(-1);
        p.timer_fds.set_rv_timeout(-1);
        p.timer_fds.set_is_timeout(-1);

        p.state = PeerConn<TCP>::State::Disconnected;
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

template <>
inline std::optional<std::string> EventLoop<UDP>::StartConnect(PeerConn<UDP>& p) {
    #ifdef DEBUG
    std::cout << "attempting to connect to peer " << p.peer_id << "\n";
    #endif
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    auto [ip_addr, port] = decode(p.peer_ip_addr); // stored in network byte order
    struct in_addr addr{ .s_addr = ip_addr };
    char ip_addr_str[INET_ADDRSTRLEN];
    if (::inet_ntop(AF_INET, &addr, ip_addr_str, sizeof(ip_addr_str)) == nullptr) {
        return "Error converting peer IP address to string";
    }
    std::string port_str = std::to_string(ntohs(port));

    if (::getaddrinfo(ip_addr_str, port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
        return "Error getting address info for peer";
    }

    p.fd = ::socket(res->ai_family,
                     res->ai_socktype | SOCK_NONBLOCK | SOCK_CLOEXEC,
                     res->ai_protocol);
    if (p.fd < 0) return "Failed to start socket";

    int yes = 1;
    const auto send_size = REQ_SIZE + sizeof(REQ_SIZE) + sizeof(RpcKind);
    const auto rcv_size = RESP_SIZE + sizeof(RESP_SIZE) + sizeof(RpcKind);
    ::setsockopt(p.fd, SOL_SOCKET, SO_SNDBUF, &send_size, sizeof(send_size));
    ::setsockopt(p.fd, SOL_SOCKET, SO_RCVBUF, &rcv_size, sizeof(rcv_size));

    int rc = ::connect(p.fd, res->ai_addr, res->ai_addrlen);

    if (rc < 0 && errno != EINPROGRESS) { ::close(p.fd); return "Failed to connect socket"; }

    p.epoll_events  = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
    p.state = PeerConn<UDP>::State::Connected;

    int ae_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int rv_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    int is_timeout_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);

    if (ae_timeout_fd < 0 || rv_timeout_fd < 0 || is_timeout_fd < 0) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn<UDP>::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to socket for peer {}:\nerror creating timer fds\n",
            p.peer_id
        );
    }

    p.timer_fds.set_ae_timeout(ae_timeout_fd);
    p.timer_fds.set_rv_timeout(rv_timeout_fd);
    p.timer_fds.set_is_timeout(is_timeout_fd);

    std::optional<const char*> peer_fd_err = register_fd(p.fd, p.epoll_events, EpollContextKind::Peer, p.peer_id);
    if (peer_fd_err) {
        ::close(p.fd);
        p.fd = -1;
        p.state = PeerConn<UDP>::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to peer {}:\n{}\n",
            p.peer_id, peer_fd_err.value()
        );
    }

    std::optional<const char*> ae_timeout_fd_err = register_fd(ae_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::AE, p.peer_id);
    std::optional<const char*> rv_timeout_fd_err = register_fd(rv_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::RV, p.peer_id);
    std::optional<const char*> is_timeout_fd_err = register_fd(is_timeout_fd, EPOLLIN | EPOLLET, EpollContextKind::PeerTimer, TimerKind::IS, p.peer_id);

    if (ae_timeout_fd_err || rv_timeout_fd_err || is_timeout_fd_err) {
        ::close(p.timer_fds.get_ae_timeout());
        ::close(p.timer_fds.get_rv_timeout());
        ::close(p.timer_fds.get_is_timeout());

        p.timer_fds.set_ae_timeout(-1);
        p.timer_fds.set_rv_timeout(-1);
        p.timer_fds.set_is_timeout(-1);

        p.state = PeerConn<UDP>::State::Disconnected;
        p.epoll_events = 0;
        return std::format(
            "Failed to connect to peer {}: failed to register timer/timeout fds\n",
            p.peer_id
        );
    }

    #ifdef DEBUG
    std::cout << "finished connecting to peer " << p.peer_id << "\n";
    #endif

    ::freeaddrinfo(res);
    return {};
}

template <>
inline std::optional<const char*> EventLoop<TCP>::OnPeerReadable(PeerConn<TCP>& p) {
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

template <>
inline std::optional<const char*> EventLoop<UDP>::OnPeerReadable(PeerConn<UDP>& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " readable\n";
    #endif

    struct msghdr msg = {0};
    struct iovec iov[1];
    iov[0].iov_base = p.rbuf;
    iov[0].iov_len = sizeof(p.rbuf);
    msg.msg_iov = iov;
    msg.msg_iovlen = 1;

    ssize_t n = ::recvmsg(p.fd, &msg, 0);
    if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) { return "failed to read peer reply: data not available\n"; }
    if (n < sizeof(uint32_t)) {
        return "received datagram too small to parse\n";
    }
    if (msg.msg_flags & MSG_TRUNC) {
        return "truncated peer reply\n";
    }

    uint32_t net_len;
    std::memcpy(&net_len, p.rbuf, sizeof(net_len));
    uint32_t msg_len = ntohl(net_len);
    size_t frame_size = msg_len + sizeof(msg_len);
    if (n < frame_size) return "peer sent oversized request\n";

    auto result = parse_rbuf(p.rbuf + sizeof(msg_len), msg_len, p.timer_fds);
    if (std::holds_alternative<const char*>(result)) return std::get<const char*>(result);

    #ifdef DEBUG
    std::cout << "passing reply from peer " << p.peer_id << " back to node\n";
    #endif
    post_node_inbox(std::move(std::get<NodeMessage>(result)));

    return {};
}

template <>
inline std::optional<const char*> EventLoop<TCP>::OnPeerWritable(PeerConn<TCP>& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " writable\n";
    #endif

    if (p.state == PeerConn<TCP>::State::Connecting) {
        int err = 0;
        socklen_t l = sizeof(err);
        if (::getsockopt(p.fd, SOL_SOCKET, SO_ERROR, &err, &l) < 0 || err != 0) {
            DropPeer(p);
            return "error attempting to write to peer socket\n";
        }
        p.state = PeerConn<TCP>::State::Connected;
        #ifdef DEBUG
        std::cout << "Set peer " << p.peer_id << " fd to Connected\n";
        #endif
        std::optional<const char*> modify_err = modify_peer_interest(p, EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET);
        if (modify_err) {
            return modify_err;
        }
    }

    itimerspec spec{};
    spec.it_value.tv_sec  = rpc_timeout_sec;
    spec.it_value.tv_nsec = rpc_timeout_nsec;
    spec.it_interval      = {0, 0};

    while (p.wbuf_offset < p.wbuf_size) {
        ssize_t n = ::send(p.fd,
            p.wbuf + p.wbuf_offset,
            p.wbuf_size - p.wbuf_offset,
            MSG_NOSIGNAL);
        if (n > 0) {
            uint8_t kind;
            std::memcpy(&kind, p.wbuf + p.wbuf_offset + sizeof(uint32_t), sizeof(kind));
            if (static_cast<RpcKind>(kind) != RpcKind::ForwardLeader) {
                ::timerfd_settime(p.timer_fds.fds[kind], 0, &spec, nullptr);
            }

            p.wbuf_offset += static_cast<size_t>(n);
            continue;
        }
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

template <>
inline std::optional<const char*> EventLoop<UDP>::OnPeerWritable(PeerConn<UDP>& p) {
    #ifdef DEBUG
    std::cout << "peer " << p.peer_id << " writable\n";
    #endif

    itimerspec spec{};
    spec.it_value.tv_sec  = rpc_timeout_sec;
    spec.it_value.tv_nsec = rpc_timeout_nsec;
    spec.it_interval      = {0, 0};

    bool drained = true;
    while (p.wbuf_size > 0) {
        uint32_t msg_len;
        std::memcpy(&msg_len, p.wbuf, sizeof(msg_len));
        msg_len = ntohl(msg_len);
        const size_t frame_size = msg_len + sizeof(msg_len);

        ssize_t n = ::send(p.fd,
            p.wbuf,
            frame_size,
            MSG_NOSIGNAL);
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            drained = false;
            break; // TODO: handle case where socket write buffer full
        }
        uint8_t kind;
        std::memcpy(&kind, p.wbuf + sizeof(msg_len), sizeof(kind));

        if (static_cast<RpcKind>(kind) != RpcKind::ForwardLeader) {
            ::timerfd_settime(p.timer_fds.fds[kind], 0, &spec, nullptr);
        }

        p.wbuf_size -= frame_size;
        if (p.wbuf_size > 0) {
            std::memmove(p.wbuf, p.wbuf + frame_size, p.wbuf_size);
        }
    }
    if (drained) {
        std::optional<const char*> modify_err = modify_peer_interest(p, p.epoll_events & ~EPOLLOUT);
        if (modify_err) return modify_err;
    }
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::OnPeerAERPCTimeout(PeerConn<T>& p) {
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
inline std::optional<const char*> EventLoop<T>::OnPeerRVRPCTimeout(PeerConn<T>& p) {
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
inline std::optional<const char*> EventLoop<T>::OnPeerISRPCTimeout(PeerConn<T>& p) {
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
inline void EventLoop<T>::DropPeer(PeerConn<T>& p) {
    #ifdef DEBUG
    std::cout << "Dropping peer " << p.peer_id << "\n";
    #endif
    if (p.fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, p.fd, nullptr);
        ::close(p.fd);
        p.fd = -1;
    }
    if (p.timer_fds.get_ae_timeout() >= 0) {
        for (int i = 0; i < sizeof(p.timer_fds.fds) / sizeof(FD); ++i) {
            FD tfd = p.timer_fds.fds[i];
            if (tfd >= 0) {
                ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, tfd, nullptr);
                ::close(tfd);
                p.timer_fds.fds[i] = -1;
            }
        }
    }

    if constexpr (T == TCP) {
        p.state        = PeerConn<TCP>::State::Disconnected;
        p.epoll_events = 0;
        std::memset(p.wbuf, 0, sizeof(p.wbuf));
        p.wbuf_offset = 0;
        p.wbuf_size = 0;
        std::memset(p.rbuf, 0, sizeof(p.rbuf));
        p.rbuf_offset = 0;
    }

    if constexpr (T == UDP) {
        p.state        = PeerConn<UDP>::State::Disconnected;
        p.epoll_events = 0;
        std::memset(p.wbuf, 0, sizeof(p.wbuf));
        p.wbuf_size = 0;
        std::memset(p.rbuf, 0, sizeof(p.rbuf));
    }
    // send message to node thread to remove this peer from its nodes list
    post_node_inbox(NodeMessage{DropPeerMsg{.source_id = p.peer_id}});
}

/* called when draining the messages in the event loop's MPSC inbox. */
template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_inflight(AppendEntriesReqPayload& payload) {
    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size() || !peer_id_to_conn[payload.dest_id]) {
        return {};
    }
    #ifdef DEBUG
    std::cout << "Posting AE RPC to outbound queue for node " << payload.dest_id << "\n";
    std::cout << "payload term = " << payload.term << "\n";
    std::cout << "payload prev log index = " << payload.prev_log_idx << "\n";
    std::cout << "payload prev log term = " << payload.prev_log_term << "\n";
    std::cout << "payload leader commit = " << payload.leader_commit << "\n";
    std::cout << "payload entries_len = " << payload.entries_len << "\n";
    #endif
    PeerConn<T>& p = peer_id_to_conn[payload.dest_id];

    if constexpr (T == TCP) {
        if (p.wbuf_offset > 0) {
            std::memmove(p.wbuf, p.wbuf + p.wbuf_offset, p.wbuf_size - p.wbuf_offset);
            p.wbuf_size -= p.wbuf_offset;
            p.wbuf_offset = 0;
        }
    }

    auto total_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    if (sizeof(p.wbuf) - p.wbuf_size < total_size) {
        return {}; // TODO: handle the case where the PeerConn's write buffer is full
    }
    BufByteWriter writer{p.wbuf + p.wbuf_size};
    writer.serialize(payload);
    p.wbuf_size += total_size;

    #ifdef DEBUG
    std::cout << "p.wbuf_size = " << p.wbuf_size << "\n";
    #endif

    if (p.state == PeerConn<T>::State::Connected) {
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
    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size() || !peer_id_to_conn[payload.dest_id]) {
        return {};
    }
    #ifdef DEBUG
    std::cout << "Posting RV RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    PeerConn<T>& p = peer_id_to_conn[payload.dest_id];

    if constexpr (T == TCP) {
        if (p.wbuf_offset > 0) {
            std::memmove(p.wbuf, p.wbuf + p.wbuf_offset, p.wbuf_size - p.wbuf_offset);
            p.wbuf_size -= p.wbuf_offset;
            p.wbuf_offset = 0;
        }
    }

    auto total_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    if (sizeof(p.wbuf) - p.wbuf_size < total_size) {
        return {}; // TODO: handle the case where the PeerConn's write buffer is full
    }
    BufByteWriter writer{p.wbuf + p.wbuf_size};
    writer.serialize(payload);
    p.wbuf_size += total_size;

    #ifdef DEBUG
    std::cout << "p.wbuf_size = " << p.wbuf_size << "\n";
    #endif

    if (p.state == PeerConn<T>::State::Connected) {
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
    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size() || !peer_id_to_conn[payload.dest_id]) {
        return {};
    }
    #ifdef DEBUG
    std::cout << "Posting IS RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    PeerConn<T>& p = peer_id_to_conn[payload.dest_id];

    if constexpr (T == TCP) {
        if (p.wbuf_offset > 0) {
            std::memmove(p.wbuf, p.wbuf + p.wbuf_offset, p.wbuf_size - p.wbuf_offset);
            p.wbuf_size -= p.wbuf_offset;
            p.wbuf_offset = 0;
        }
    }

    auto total_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    if (sizeof(p.wbuf) - p.wbuf_size < total_size) {
        return {}; // TODO: handle the case where the PeerConn's write buffer is full
    }
    BufByteWriter writer{p.wbuf + p.wbuf_size};
    writer.serialize(payload);
    p.wbuf_size += total_size;

    #ifdef DEBUG
    std::cout << "p.wbuf_size = " << p.wbuf_size << "\n";
    #endif

    if (p.state == PeerConn<T>::State::Connected) {
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
    if (payload.dest_id < 0 || payload.dest_id >= peer_id_to_conn.size() || !peer_id_to_conn[payload.dest_id]) {
        return {};
    }
    #ifdef DEBUG
    std::cout << "Posting FL RPC to outbound queue for node " << payload.dest_id << "\n";
    #endif
    PeerConn<T>& p = peer_id_to_conn[payload.dest_id];

    if constexpr (T == TCP) {
        if (p.wbuf_offset > 0) {
            std::memmove(p.wbuf, p.wbuf + p.wbuf_offset, p.wbuf_size - p.wbuf_offset);
            p.wbuf_size -= p.wbuf_offset;
            p.wbuf_offset = 0;
        }
    }

    auto total_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    if (sizeof(p.wbuf) - p.wbuf_size < total_size) {
        return {}; // TODO: handle the case where the PeerConn's write buffer is full
    }
    BufByteWriter writer{p.wbuf + p.wbuf_size};
    writer.serialize(payload);
    p.wbuf_size += total_size;

    #ifdef DEBUG
    std::cout << "p.wbuf_size = " << p.wbuf_size << "\n";
    #endif

    if (p.state == PeerConn<T>::State::Connected) {
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

// template <SocketType T>
// inline std::optional<std::string> EventLoop<T>::arm_heartbeat_timer(NodeID peer_id) {
//     #ifdef DEBUG
//     std::cout << "arming heartbeat timer for peer " << peer_id << "\n";
//     #endif

//     if (peer_id < 0 || peer_id >= peer_id_to_conn.size()) {
//         return std::format(
//             "Failed to arm heartbeat timer: peer id {} not found in peer_conns\n",
//             peer_id);
//     }
//     PeerConn<T>& p = peer_id_to_conn[peer_id];
//     if (!p) {
//         return std::format(
//             "Failed to arm heartbeat timer: peer id {} not found in peer_conns\n",
//             peer_id);
//     }

//     // Periodic timer: it_value == it_interval == period. The first
//     // expiration lands `period` from now; subsequent ones fire at the
//     // same cadence until disarmed.
//     constexpr long NS_PER_SEC = 1'000'000'000;
//     itimerspec spec{};
//     spec.it_value.tv_sec  = heartbeat_period_sec;
//     spec.it_value.tv_nsec = heartbeat_period_nsec;
//     spec.it_interval      = spec.it_value;
//     ::timerfd_settime(p.timer_fds.get_heartbeat(), 0, &spec, nullptr);
//     return {};
// }

// template <SocketType T>
// inline std::optional<std::string> EventLoop<T>::disarm_heartbeat_timer(NodeID peer_id) {
//     #ifdef DEBUG
//     std::cout << "disarming heartbeat timer for node " << peer_id << "\n";
//     #endif

//     if (peer_id < 0 || peer_id >= peer_id_to_conn.size()) {
//         return std::format(
//             "Failed to disarm heartbeat timer: peer id {} not found in peer_conns\n",
//             peer_id);
//     }
//     PeerConn<T>& p = peer_id_to_conn[peer_id];
//     if (!p) {
//         return std::format(
//             "Failed to disarm heartbeat timer: peer id {} not found in peer_conns\n",
//             peer_id);
//     }

//     // Zero spec disarms
//     // Drain any already-counted expirations so that
//     // EPOLLET doesn't deliver a stale read after we return.
//     if (p.timer_fds.get_heartbeat() == -1) return {};

//     itimerspec zero{};
//     ::timerfd_settime(p.timer_fds.get_heartbeat(), 0, &zero, nullptr);
//     uint64_t dummy;
//     ssize_t n = ::read(p.timer_fds.get_heartbeat(), &dummy, sizeof(dummy));
//     (void)n;

//     return {};
// }
