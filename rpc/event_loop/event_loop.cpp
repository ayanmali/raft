#include "./event_loop.hpp"
#include <cstddef>
#include <fcntl.h>
#ifdef DEBUG
#include <iostream>
#endif

EventLoop::EventLoop(FD listen_fd, size_t inbound_cap, NodeInbox& node_inbox, size_t this_id, long period) :
listen_fd{listen_fd},
client_slab{inbound_cap},
node_inbox{node_inbox},
this_id{this_id},
period{period}
{}

std::expected<std::unique_ptr<EventLoop>, std::string> EventLoop::CreateEventLoop(FD listen_fd, size_t inbound_cap, NodeInbox& node_inbox, size_t this_id, long period) {
    auto loop = std::unique_ptr<EventLoop>(new EventLoop(listen_fd, inbound_cap, node_inbox, this_id, period));

    loop->epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (loop->epoll_fd < 0) return Unexpected("epoll_create1 failed");

    loop->event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (loop->event_fd < 0) return Unexpected("eventfd failed");

    VoidExpected non_blocking_ok = loop->set_nonblocking(loop->listen_fd);
    if (!non_blocking_ok) return UnexpectedF(
        std::format("error initializing event loop:\n{}\n", non_blocking_ok.error())
    );

    VoidExpected register_ok = loop->register_fd(loop->listen_fd, EPOLLIN | EPOLLET);
    if (!register_ok) return UnexpectedF(
        std::format("error initializing event loop; listen fd registration failed:\n{}\n", register_ok.error())
    );

    register_ok = loop->register_fd(loop->event_fd, EPOLLIN | EPOLLET );
    if (!register_ok) return UnexpectedF(
        std::format("error initializing event loop; event fd registration failed:\n{}\n", register_ok.error())
    );
    return loop;
}

EventLoop::~EventLoop() {
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }
    // Listen fd is owned by Node.

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

VoidExpected EventLoop::Run() {
    #ifdef DEBUG
    std::cout << "starting event loop with id " << this_id << "\n";
    #endif
    epoll_event evs[EPOLL_BATCH];
    while (!stopped.load(std::memory_order_acquire)) {
        #ifdef DEBUG
        std::cout << "waiting for events...\n";
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
                OnEventFd();
                continue;
            }

            if (auto it = client_fd_to_id.find(fd); it != client_fd_to_id.end()) {
                ClientConn* c = client_conns.at(it->second);
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    #ifdef DEBUG
                    std::cout << "epoll error found for client " << it->second << ":\n";
                    #endif
                    CloseClient(c);
                    continue;
                }
                if (e & EPOLLIN) {
                    #ifdef DEBUG
                    std::cout << "new client message from client " << c->id << "\n";
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
                    std::cout << "ready to send reply to client " << c->id << "\n";
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
                    std::cout << "disconnecting peer " << p.peer_id << "\n";
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

void EventLoop::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    wake_armed.store(false, std::memory_order_release);

    std::unique_ptr<RaftMessage> out;
    bool flag = outbound_inbox.PopOne(&out);
    while (flag) {
        std::visit([&out, this](auto&& payload) {
            using T = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<T, AppendEntriesReqPayload> || std::is_same_v<T, RequestVoteReqPayload> || std::is_same_v<T, InstallSnapshotReqPayload>) {
                post_inflight(payload, out->node_id);
            }

            else if constexpr (std::is_same_v<T, AppendEntriesRespPayload> || std::is_same_v<T, RequestVoteRespPayload> || std::is_same_v<T, InstallSnapshotRespPayload>) {
                post_reply(payload, out->node_id);
            }

            else if constexpr (std::is_same_v<T, ArmTimerPayload>) {
                arm_peer_timer(out->node_id);
            }

            else if constexpr (std::is_same_v<T, DisarmTimerPayload>) {
                disarm_peer_timer(out->node_id);
            }

            else if constexpr (std::is_same_v<T, HeartbeatTimeoutPayload>) {
                // This payload is only sent from EventLoop to Node.
            }

            else static_assert(false, "non-exhaustive visitor!");
        }, out->data);
        flag = outbound_inbox.PopOne(&out);
    }

}

// drains this event loop's MPSC inbox
// for each message in the inbox, enqueue onto the corresponding peer's RPC outbox
void EventLoop::OnEventFd() {
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

bool EventLoop::post_node_inbox(RpcRequest& req, NodeID client_id) {
    int counter = 0;


    while (counter < MAX_ATTEMPTS) {
        bool res = node_inbox.Push(this_id, std::make_unique<RaftMessage>(std::visit([](auto&& payload) -> RpcMessage {
            return payload;
        }, req), client_id));
        ++counter;
        if (res) return true;
    }
    return false;
};

bool EventLoop::post_node_inbox(RpcReply& reply, NodeID peer_id) {
    int counter = 0;

    while (counter < MAX_ATTEMPTS) {
        bool res = node_inbox.Push(this_id, std::make_unique<RaftMessage>(std::visit([](auto&& payload) -> RpcMessage {
            return payload;
        }, reply), peer_id));
        ++counter;
        if (res) return true;
    }
    return false;
};
