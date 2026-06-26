#include "./event_loop.hpp"
#include <cstddef>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <format>
#ifdef DEBUG
#include <iostream>
#endif

EventLoop::EventLoop(size_t inbound_cap, NodeInbox& node_inbox, size_t this_id, long heartbeat_period) :
client_slab{inbound_cap},
node_inbox{node_inbox},
this_id{this_id},
heartbeat_period{heartbeat_period}
{}

std::expected<std::unique_ptr<EventLoop>, std::string> EventLoop::CreateEventLoop(size_t inbound_cap, NodeInbox& node_inbox, size_t this_id, long heartbeat_period) {
    auto loop = std::unique_ptr<EventLoop>(new EventLoop(inbound_cap, node_inbox, this_id, heartbeat_period));

    // Epoll fd
    loop->epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (loop->epoll_fd < 0) return Unexpected("epoll_create1 failed");

    // Listening socket
    VoidExpected listen_ok = loop->setup_listen_socket();
    if (!listen_ok) return Unexpected("listen socket could not be set up");
    VoidExpected non_blocking_ok = loop->set_nonblocking(loop->listen_fd);
    if (!non_blocking_ok) return UnexpectedF(
        std::format("error initializing event loop:\n{}\n", non_blocking_ok.error())
    );
    VoidExpected register_ok = loop->register_fd(loop->listen_fd, EPOLLIN | EPOLLET);
    if (!register_ok) return UnexpectedF(
        std::format("error initializing event loop; listen fd registration failed:\n{}\n", register_ok.error())
    );

    // Cross-thread event fd
    loop->event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (loop->event_fd < 0) return Unexpected("eventfd failed");
    register_ok = loop->register_fd(loop->event_fd, EPOLLIN | EPOLLET);
    if (!register_ok) return UnexpectedF(
        std::format("error initializing event loop; event fd registration failed:\n{}\n", register_ok.error())
    );

    return loop;
}

EventLoop::~EventLoop() {
    if (listen_fd >= 0) ::close(listen_fd);
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }

};

void EventLoop::Stop() {
    stopped.store(true, std::memory_order_release);
    wake_eventfd_unconditional();
}

void EventLoop::Wake() {
    if (!wake_armed.exchange(true, std::memory_order_acq_rel)) {
        wake_eventfd_unconditional();
    }

}

void EventLoop::wake_eventfd_unconditional() {
    #ifdef DEBUG
    std::cout << "waking event loop " << this_id << "\n";
    #endif
    uint64_t one = 1;
    ssize_t  n   = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

VoidExpected EventLoop::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return Unexpected("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return Unexpected("fcntl F_SETFL O_NONBLOCK failed");
    }
    return {};
}

VoidExpected EventLoop::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return Unexpected("epoll_ctl ADD failed");
    }
    return {};
}

VoidExpected EventLoop::setup_listen_socket() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    // TODO: fix this?
    if (::getaddrinfo(nullptr, SERVER_PORT, &hints, &res) != 0 || res == nullptr) {
        return Unexpected("getaddrinfo failed");
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> res_guard(res, &::freeaddrinfo);

    listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) return Unexpected("socket failed");

    int yes = 1;
    // SO_REUSEPORT must be set BEFORE bind() so that all N sockets
    // bound to the same port are members of the same SO_REUSEPORT
    // group; the kernel then load-balances incoming connections
    // across them.
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    ::setsockopt(listen_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    addrinfo* p = nullptr;
    for (p = res; p; p = p->ai_next) {
        if (::bind(listen_fd, p->ai_addr, p->ai_addrlen) == 0) break;
    }
    if (!p) { ::close(listen_fd); return Unexpected("bind failed"); }
    if (::listen(listen_fd, SERVER_BACKLOG) != 0) {
        ::close(listen_fd);
        return Unexpected("listen failed");
    }
    return {};
}

VoidExpectedF EventLoop::Run() {
    #ifdef DEBUG
    std::cout << "starting event loop with id " << this_id << "\n";
    #endif
    epoll_event evs[EPOLL_BATCH];
    while (!stopped.load(std::memory_order_acquire)) {
        #ifdef DEBUG
        std::cout << "---\nwaiting for events...\n";
        #endif
        int n = ::epoll_wait(epoll_fd, evs, EPOLL_BATCH, -1);

        if (n < 0) {
            if (errno == EINTR) continue;
            return Unexpected("epoll_wait failed");
        }

        // loop over all ready FDs
        #ifdef DEBUG
        std::cout << "found " << n << " ready fds\n";
        #endif

        for (int i = 0; i < n; ++i) {
            const FD fd = evs[i].data.fd;
            const uint32_t e = evs[i].events;
            #ifdef DEBUG
            std::cout << i << "th fd:\n";
            #endif

            if (fd == listen_fd) {
                #ifdef DEBUG
                std::cout << "accepting new client connection\n";
                #endif
                VoidExpected accept_ok = Accept();
                if (!accept_ok) std::cout << "failed to accept new client connection; skipping:\n" << accept_ok.error() << "\n";
                continue;
            }
            if (fd == event_fd) {
                #ifdef DEBUG
                std::cout << "event fd awakened\n";
                #endif
                VoidExpectedF on_event_fd_ok = OnEventFd();
                if (!on_event_fd_ok) {
                    return UnexpectedF(std::format(
                        "Failed to process new event:\n{}\n",
                        on_event_fd_ok.error()
                    ));
                }
                continue;
            }

            if (auto it = client_conns.find(fd); it != client_conns.end()) {
                ClientConn* c = it->second;
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    #ifdef DEBUG
                    std::cout << "epoll error found for client " << it->second << ":";
                    if (e & EPOLLERR) {
                        std::cout << "EPOLLERR\n";
                    }
                    else if (e & EPOLLHUP) {
                        std::cout << "EPOLLHUP\n";
                    }
                    else if (e & EPOLLRDHUP) {
                        std::cout << "EPOLLRDHUP\n";
                    }
                    #endif
                    CloseClient(c);
                    continue;
                }
                if (e & EPOLLIN) {
                    #ifdef DEBUG
                    std::cout << "new client message from client with fd " << fd << "\n";
                    #endif
                    VoidExpected readable_ok = OnClientReadable(c);
                    if (!readable_ok) {
                        #ifdef DEBUG
                        std::cout << "failed to read incoming client message:\n" << readable_ok.error() << "\n";
                        #endif
                        continue;
                    }
                }
                if (e & EPOLLOUT) {
                    #ifdef DEBUG
                    std::cout << "ready to send reply to client with fd " << fd << "\n";
                    #endif
                    VoidExpected writable_ok = OnClientWritable(c);
                    if (!writable_ok) {
                        #ifdef DEBUG
                        std::cout << "failed to write to client socket:\n" << writable_ok.error() << "\n";
                        #endif
                        continue;
                    }
                }
                continue;
            }

            if (auto it = peer_fd_to_id.find(fd); it != peer_fd_to_id.end()) {
                PeerConn& p = peer_conns.at(it->second);
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    #ifdef DEBUG
                    std::cout << "event loop received epoll error ";
                    if (e & EPOLLERR) {
                        std::cout << "EPOLLERR";
                    }
                    else if (e & EPOLLHUP) {
                        std::cout << "EPOLLHUP";
                    }
                    else if (e & EPOLLRDHUP) {
                        std::cout << "EPOLLRDHUP";
                    }
                    std::cout << " for peer " << p.peer_id << "; disconnecting peer\n";
                    #endif
                    DropPeer(p);
                    continue;
                }
                if (e & EPOLLIN) {
                    #ifdef DEBUG
                    std::cout << "obtained reply from peer " << p.peer_id << "\n";
                    #endif
                    VoidExpected readable_ok = OnPeerReadable(p);
                    if (!readable_ok) {
                        #ifdef DEBUG
                        std::cout << "failed to read incoming peer reply:\n" << readable_ok.error() << "\n";
                        #endif
                        continue;
                    }
                }
                if (e & EPOLLOUT) {
                    #ifdef DEBUG
                    std::cout << "ready to send RPC to peer " << p.peer_id << "\n";
                    #endif
                    VoidExpected writable_ok = OnPeerWritable(p);
                    if (!writable_ok) {
                        #ifdef DEBUG
                        std::cout << "failed to write RPC to peer socket:\n" << writable_ok.error() << "\n";
                        #endif
                        continue;
                    }
                }
                continue;
            }

            if (auto it = peer_timer_fd_to_id.find(fd); it != peer_timer_fd_to_id.end()) {
                PeerConn& p = peer_conns.at(it->second);
                if (e & EPOLLIN) {
                    #ifdef DEBUG
                    std::cout << "timer fd fired for peer " << p.peer_id << "\n";
                    #endif
                    VoidExpected peer_timer_ok = OnPeerTimer(p);
                    if (!peer_timer_ok) {
                        #ifdef DEBUG
                        std::cout << "failed to post heartbeat payload to node inbox:\n" << peer_timer_ok.error() << "\n";
                        #endif
                        continue;
                    }
                }
            }
        }
    }
    return {};
}

/* Cross-thread messaging */

VoidExpectedF EventLoop::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    #ifdef DEBUG
    std::cout << "draining inbox...\n";
    #endif
    wake_armed.store(false, std::memory_order_release);

    std::unique_ptr<RaftMessage> out;
    bool flag = outbound_inbox.PopOne(&out);
    while (flag) {
        VoidExpectedF ok = std::visit([&out, this](auto&& payload) -> VoidExpectedF {
            using T = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<T, AppendEntriesReqPayload> || std::is_same_v<T, RequestVoteReqPayload> || std::is_same_v<T, InstallSnapshotReqPayload>) {
                #ifdef DEBUG
                std::cout << "found request\n";
                #endif
                VoidExpectedF post_ok = post_inflight(payload, out->node_id);
                if (!post_ok) {
                    return UnexpectedF(std::format(
                        "Error while draining event loop inbox - failed to post RPC to inflight queue for peer {}:\n{}",
                        out->node_id, post_ok.error()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, AppendEntriesRespPayload> || std::is_same_v<T, RequestVoteRespPayload> || std::is_same_v<T, InstallSnapshotRespPayload>) {
                #ifdef DEBUG
                std::cout << "found reply\n";
                #endif
                VoidExpectedF post_ok = post_reply(payload, out->node_id);
                if (!post_ok) {
                    return UnexpectedF(std::format(
                        "Error while draining event loop inbox - failed to post reply to inflight queue for peer {}:\n{}",
                        out->node_id, post_ok.error()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, ArmTimer>) {
                #ifdef DEBUG
                std::cout << "found arm timer req\n";
                #endif
                VoidExpectedF arm_ok = arm_heartbeat_timer(out->node_id);
                if (!arm_ok) {
                    return UnexpectedF(std::format(
                        "Error while draining inbox: failed to arm heartbeat timer for node {}:\n{}\n",
                        out->node_id, arm_ok.error()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, DisArmTimer>) {
                #ifdef DEBUG
                std::cout << "found disarm timer req\n";
                #endif
                VoidExpectedF disarm_ok = disarm_heartbeat_timer(out->node_id);
                if (!disarm_ok) {
                    return UnexpectedF(std::format(
                        "Error while draining inbox: failed to disarm heartbeat timer for node {}:\n{}\n",
                        out->node_id, disarm_ok.error()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, AddPeerMsg>) {
                #ifdef DEBUG
                std::cout << "found add peer msg\n";
                #endif
                VoidExpectedF add_peer_ok = AddPeer(out->node_id, payload.ip_addr, payload.port);
                return add_peer_ok;
            }

            else if constexpr (std::is_same_v<T, HeartbeatTimeout> || std::is_same_v<T, DropPeerMsg>){
                // These payloads are only sent from EventLoop to Node.
            }

            else static_assert(false, "non-exhaustive visitor!");
            return {};
        }, out->data);
        flag = outbound_inbox.PopOne(&out);
    }
    return {};
}

// drains this event loop's MPSC inbox
// for each message in the inbox, enqueue onto the corresponding peer's RPC outbox
VoidExpectedF EventLoop::OnEventFd() {
    uint64_t counter;
    for (;;) {
        ssize_t n = ::read(event_fd, &counter, sizeof(counter));
        if (n == sizeof(counter)) break;
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        break;
    }
    VoidExpectedF drain_ok = DrainInbox();
    if (!drain_ok) {
        return UnexpectedF(std::format(
            "Error while draining inbox:\n{}\n",
            drain_ok.error()
        ));
    }
    return {};
}
