#include "./event_loop.hpp"
#include "../protocol/utils.hpp"
#include <arpa/inet.h>
#include <format>
#include <netinet/tcp.h>

#ifdef DEBUG
#include <iostream>
#endif

VoidExpected EventLoop::modify_client_interest(ClientConn* c, uint32_t events) {
    if (c->epoll_events == events) return {};
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = c->fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c->fd, &ev) < 0) {
        CloseClient(c);
        return Unexpected("Error modifying events for client fd");
    }
    c->epoll_events = events;
    return {};
}

VoidExpected EventLoop::Accept() {
    for (;;) {
        ClientConn* c = client_slab.Acquire();
        if (!c) continue;

        sockaddr_in peer{};
        socklen_t   plen = sizeof(peer);
        FD fd = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer),
                          &plen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return {};
            if (errno == EINTR) continue;
            return {}; // transient errors: drop and try again on next epoll wake
        }

        // populate client ip address
        inet_ntop(AF_INET, &peer.sin_addr, c->client_ip_addr, sizeof(c->client_ip_addr));

        #ifdef DEBUG
        std::cout << "connection accepted from node w/ IP address " << c->client_ip_addr << "\n";
        #endif

        int yes = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        c->fd = fd;
        c->epoll_events = EPOLLIN | EPOLLRDHUP | EPOLLET;

        VoidExpected client_fd_ok = register_fd(fd, c->epoll_events);
        if (!client_fd_ok) {
            #ifdef DEBUG
            std::cout << "error accepting client connection:\n" << client_fd_ok.error() << "\n";
            #endif
            ::close(fd);
            client_slab.Release(c);
            continue;
        }
        client_conns[c->fd] = c;
    }
    return {};
}

// TODO: use fixed size stack buffers instead of vectors
VoidExpected EventLoop::OnClientWritable(ClientConn* c) {
    #ifdef DEBUG
    std::cout << "client with ip " << c->client_ip_addr << " writable\n";
    #endif
    while (c->wbuf_offset < c->wbuf.size()) {
        #ifdef DEBUG
        std::cout << "sending reply to client with ip " << c->client_ip_addr << "\n";
        #endif
        ssize_t n = ::send(
            c->fd,
            c->wbuf.data() + c->wbuf_offset,
            c->wbuf.size() - c->wbuf_offset,
            MSG_NOSIGNAL);
        if (n > 0) { c->wbuf_offset += static_cast<size_t>(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return {};
        CloseClient(c);
        return {};
    }
    c->wbuf.clear();
    c->wbuf_offset = 0;
    VoidExpected modify_ok = modify_client_interest(c, c->epoll_events & ~EPOLLOUT);
    if (!modify_ok) {
        #ifdef DEBUG
        std::cout << modify_ok.error() << "\n";
        #endif
    }
    // if (c.closing && c.pending_tasks == 0) ReapClient(c);
    if (c->closing) {
        client_conns.erase(c->fd);
        client_slab.Release(c);
    }
    return modify_ok;
}

VoidExpected EventLoop::OnClientReadable(ClientConn* c) {
    // TODO: if latency is too high here, replace c.rbuf w a ring buffer, or use readv
    #ifdef DEBUG
    std::cout << "client with ip " << c->client_ip_addr << " readable\n";
    #endif
    for (;;) {
        size_t old = c->rbuf.size();
        c->rbuf.resize(old + RECV_CHUNK);
        ssize_t n = ::recv(c->fd, c->rbuf.data() + old, RECV_CHUNK, 0);

        if (n > 0) { c->rbuf.resize(old + n); continue; }
        if (n == 0) {
            c->rbuf.resize(old);
            CloseClient(c);
            break;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) { c->rbuf.resize(old); break; }

        CloseClient(c);
        return Unexpected("unexpected error attempting to read client message\n");
    }

    // drain as many complete request frames as the buffer can hold
    while (!c->closing && !c->rbuf.empty()) {
        #ifdef DEBUG
        std::cout << "reading request" << "\n";
        #endif

        size_t before = c->rbuf.size();

        auto request_raw = parse_rbuf(c); // erases the read bytes in rbuf
        if (!request_raw) {
            //CloseClient(c);
            return Unexpected(request_raw.error());
        }
        RpcMessage& req = request_raw.value();
        #ifdef DEBUG
        std::cout << "posting inbound request from client with client_ip_addr " << c->client_ip_addr << " to node inbox\n";
        #endif
        post_node_inbox(std::move(req));

        if (c->rbuf.size() == before) break; // need more bytes
    }
    return {};
}

void EventLoop::CloseClient(ClientConn* c) {
    #ifdef DEBUG
    std::cout << "closing client with ip " << c->client_ip_addr << "\n";
    #endif
    if (c->closing) return;
    c->closing = true;

    if (c->fd >= 0) {
        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->fd, nullptr);
        ::close(c->fd);
        client_conns.erase(c->fd);
    }

    client_slab.Release(c);
    //if (c.pending_tasks == 0) ReapClient(c);
}

VoidExpectedF EventLoop::post_reply(AppendEntriesRespPayload& payload) {
    #ifdef DEBUG
    std::cout << "posting AE reply to outbound queue to node " << payload.client_id << "\n";
    #endif
    auto it = client_conns.find(payload.client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", payload.client_id)
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
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::post_reply(RequestVoteRespPayload& payload) {
    #ifdef DEBUG
    std::cout << "posting RV reply to outbound queue to node " << payload.client_id << "\n";
    #endif
    auto it = client_conns.find(payload.client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", payload.client_id)
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
        Wake();
    }
    return {};
}

VoidExpectedF EventLoop::post_reply(InstallSnapshotRespPayload& payload) {
    #ifdef DEBUG
    std::cout << "posting IS reply to outbound queue to node " << payload.client_id << "\n";
    #endif
    auto it = client_conns.find(payload.client_id);
    if (it == client_conns.end()) {
        return UnexpectedF(
            std::format("client id {} not found\n", payload.client_id)
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
        Wake();
    }
    return {};
}
