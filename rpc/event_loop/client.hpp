#pragma once
#include "./event_loop.hpp"
#include "../protocol/utils.hpp"
#include <format>
#include <arpa/inet.h>
#include <netinet/tcp.h>

#ifdef DEBUG
#include <iostream>
#endif

template <>
inline void EventLoop<TCP>::CloseClient(ClientConn<TCP>* c) {
    #ifdef DEBUG
    std::cout << "closing client with ip " << c->client_ip_addr << "\n";
    #endif
    if (c->closing) return;
    c->closing = true;

    if (c->fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->fd, nullptr);
        ::close(c->fd);
        client_data.client_fd_to_ip.erase(c->fd);
        client_data.client_ip_to_conn.erase(c->client_ip_addr);
    }

    client_slab.Release(c);
    //if (c.pending_tasks == 0) ReapClient(c);
}

template <>
inline std::optional<const char*> EventLoop<TCP>::modify_client_interest(ClientConn<TCP>* c, uint32_t events) {
    if (c->epoll_events == events) return {};
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 = (static_cast<uint64_t>(EpollContextKind::Client) << 56)
                |  static_cast<uint64_t>(c->fd);
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        CloseClient(c);
        return "Error modifying events for client fd";
    }
    c->epoll_events = events;
    return {};
}

template <>
inline std::optional<const char*> EventLoop<UDP>::modify_listener_interest(uint32_t events) {
    if (listen_epoll_events == events) return {};
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 = (static_cast<uint64_t>(EpollContextKind::Listen) << 56)
                |  static_cast<uint64_t>(listen_fd);
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, listen_fd, &ev) < 0) {
        return "Error modifying events for listen fd";
    }
    listen_epoll_events = events;
    return {};
}

template <>
inline std::optional<const char*> EventLoop<TCP>::Accept() {
    for (int i = 0; i < MAX_ATTEMPTS; ++i) {
        sockaddr_in peer{};
        socklen_t   plen = sizeof(peer);
        FD fd = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer),
                          &plen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return {};
            if (errno == EINTR) continue;
            return {}; // transient errors: drop and try again on next epoll wake
        }

        ClientConn<TCP>* c = client_slab.Acquire();
        if (!c) continue;

        // populate client ip address
        c->client_ip_addr = encode(peer.sin_addr.s_addr, peer.sin_port);

        #ifdef DEBUG
        std::cout << "connection accepted from node w/ IP address " << c->client_ip_addr << "\n";
        #endif

        int yes = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        c->fd = fd;
        c->epoll_events = EPOLLIN | EPOLLRDHUP | EPOLLET;

        std::optional<const char*> client_fd_err = register_fd(fd, c->epoll_events, EpollContextKind::Client);
        if (client_fd_err) {
            #ifdef DEBUG
            std::cout << "error accepting client connection:\n" << client_fd_err.value() << "\n";
            #endif
            ::close(fd);
            client_slab.Release(c);
            continue;
        }

        client_data.client_fd_to_ip[c->fd] = c->client_ip_addr;
        client_data.client_ip_to_conn[c->client_ip_addr] = c;
    }
    return {};
}

template <>
inline std::optional<const char*> EventLoop<TCP>::OnClientWritable(ClientConn<TCP>* c) {
    #ifdef DEBUG
    std::cout << "client with ip " << c->client_ip_addr << " writable\n";
    #endif
    while (c->wbuf_offset < c->wbuf_size) {
        #ifdef DEBUG
        std::cout << "sending reply to client with ip " << c->client_ip_addr << "\n";
        #endif
        ssize_t n = ::send(
            c->fd,
            c->wbuf + c->wbuf_offset,
            c->wbuf_size - c->wbuf_offset,
            MSG_NOSIGNAL);
        if (n > 0) { c->wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
        CloseClient(c);
        return {};
    }
    std::memset(c->wbuf, 0, c->wbuf_size);
    c->wbuf_offset = 0;
    std::optional<const char*> modify_err = modify_client_interest(c, c->epoll_events & ~EPOLLOUT);
    if (modify_err) {
        #ifdef DEBUG
        std::cout << modify_err.value() << "\n";
        #endif
    }
    // if (c.closing && c.pending_tasks == 0) ReapClient(c);
    if (c->closing) {
        client_data.client_fd_to_ip.erase(c->fd);
        client_data.client_ip_to_conn.erase(c->client_ip_addr);
        client_slab.Release(c);
    }
    return modify_err;
}

template <>
inline std::optional<const char*> EventLoop<TCP>::OnClientReadable(ClientConn<TCP>* c) {
    // TODO: if latency is too high here, replace c.rbuf w a ring buffer, or use readv
    #ifdef DEBUG
    std::cout << "client with ip " << c->client_ip_addr << " readable\n";
    #endif
    size_t end = c->rbuf_offset;
    for (;;) {
        ssize_t n = ::recv(c->fd, c->rbuf + end, sizeof(c->rbuf) - end, 0);
        if (n > 0) { end += n; continue; }
        if (n == 0) {
            //CloseClient(c);
            break;
            // return {};
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }
        CloseClient(c);
        return "unexpected error attempting to read client message\n";
    }

    // drain as many complete request frames as the buffer can hold
    size_t parsed = 0;
    while (!c->closing && end - parsed >= sizeof(uint32_t)) {
        #ifdef DEBUG
        std::cout << "reading request\n";
        #endif

        uint32_t net_len;
        std::memcpy(&net_len, c->rbuf + parsed, sizeof(net_len));
        uint32_t msg_len = ntohl(net_len);
        if (msg_len > sizeof(c->rbuf) - sizeof(uint32_t)) {
            CloseClient(c);
            return "client sent oversized request frame\n";
        }
        size_t frame_size = msg_len + sizeof(msg_len);
        if (end - parsed < frame_size) break;

        auto request_raw = parse_rbuf(c, msg_len, parsed); // erases the read bytes in rbuf
        if (std::holds_alternative<const char*>(request_raw)) {
            CloseClient(c);
            return std::get<const char*>(request_raw);
        }
        parsed += frame_size;

        #ifdef DEBUG
        std::cout << "posting inbound request from client with client_ip_addr " << c->client_ip_addr << " to node inbox\n";
        #endif
        post_node_inbox(std::move(std::get<NodeMessage>(request_raw)));
    }
    c->rbuf_offset = end - parsed;
    if (c->rbuf_offset > 0) std::memmove(c->rbuf, c->rbuf + parsed, c->rbuf_offset);
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_reply(AppendEntriesRespPayload& payload) {
    if (!client_data.client_ip_to_conn.contains(payload.client_ip_addr)) {
        return std::format(
            "failed to post AE reply: payload IP + port token {} not found in client connections map",
            payload.client_ip_addr
        );
    }
    ClientConn<T>* c = client_data.client_ip_to_conn[payload.client_ip_addr];

    #ifdef DEBUG
    std::cout << "posting AE reply to outbound queue to node w/ ip " << c->client_ip_addr << "\n";
    #endif
    //++c.pending_tasks;

    c->wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    BufByteWriter writer{c->wbuf};
    writer.serialize(payload);

    if constexpr (T == TCP) {
        if (c->wbuf_offset < c->wbuf_size) {
            std::optional<const char*> modify_err = modify_client_interest(c, c->epoll_events | EPOLLOUT);
            if (modify_err) {
                return modify_err.value();
            }
            Wake();
        }
    }
    if constexpr (T == UDP) {
        std::optional<const char*> modify_err = modify_listener_interest(listen_epoll_events | EPOLLOUT);
        if (modify_err) {
            return modify_err.value();
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_reply(RequestVoteRespPayload& payload) {
    if (!client_data.client_ip_to_conn.contains(payload.client_ip_addr)) {
        return std::format(
            "failed to post RV reply: payload IP + port token {} not found in client connections map",
            payload.client_ip_addr
        );
    }
    ClientConn<T>* c = client_data.client_ip_to_conn[payload.client_ip_addr];

    #ifdef DEBUG
    std::cout << "posting RV reply to outbound queue to node w/ ip " << c->client_ip_addr << "\n";
    #endif

    c->wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    BufByteWriter writer{c->wbuf};
    writer.serialize(payload);

    if constexpr (T == TCP) {
        if (c->wbuf_offset < c->wbuf_size) {
            std::optional<const char*> modify_err = modify_client_interest(c, c->epoll_events | EPOLLOUT);
            if (modify_err) {
                return modify_err.value();
            }
            Wake();
        }
    }
    if constexpr (T == UDP) {
        std::optional<const char*> modify_err = modify_listener_interest(listen_epoll_events | EPOLLOUT);
        if (modify_err) {
            return modify_err.value();
        }
        Wake();
    }
    return {};
}

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::post_reply(InstallSnapshotRespPayload& payload) {
    if (!client_data.client_ip_to_conn.contains(payload.client_ip_addr)) {
        return std::format(
            "failed to post IS reply: payload IP + port token {} not found in client connections map",
            payload.client_ip_addr
        );
    }
    ClientConn<T>* c = client_data.client_ip_to_conn[payload.client_ip_addr];

    #ifdef DEBUG
    std::cout << "posting IS reply to outbound queue to node w/ ip " << c->client_ip_addr << "\n";
    #endif
    //++c.pending_tasks;

    c->wbuf_size = payload.size() + sizeof(uint32_t) + sizeof(RpcKind);
    BufByteWriter writer{c->wbuf};
    writer.serialize(payload);


    if constexpr (T == TCP) {
        if (c->wbuf_offset < c->wbuf_size) {
            std::optional<const char*> modify_err = modify_client_interest(c, c->epoll_events | EPOLLOUT);
            if (modify_err) {
                return modify_err.value();
            }
            Wake();
        }
    }
    if constexpr (T == UDP) {
        std::optional<const char*> modify_err = modify_listener_interest(listen_epoll_events | EPOLLOUT);
        if (modify_err) {
            return modify_err.value();
        }
        Wake();
    }
    return {};
}
