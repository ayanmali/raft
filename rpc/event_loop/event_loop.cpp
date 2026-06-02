#include "./event_loop.hpp"
#include <cstddef>

EventLoop::EventLoop(FD listen_fd, 
                    size_t inbound_cap, 
                    NodeInbox& node_inbox_, 
                    size_t this_id_) 
: listen_fd(listen_fd),
  client_slab(inbound_cap),
  node_inbox(node_inbox_),
  this_id(this_id_) {
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

void EventLoop::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
    }
}

void EventLoop::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        throw std::runtime_error("epoll_ctl ADD failed");
    }
}

void EventLoop::modify_client_interest(ClientConn& c, uint32_t events) {
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

void EventLoop::modify_peer_interest(PeerConn& p, uint32_t events) {
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

void EventLoop::Run() {
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

/* Cross-thread messaging */

void EventLoop::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    wake_armed.store(false, std::memory_order_release);

    outbound_inbox.DrainAll([](std::unique_ptr<RaftMessage>&& out) {
        std::visit([](auto&& payload)
        {
            using T = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<T, AppendEntriesReqPayload>)
            || constexpr (std::is_same_v<T, RequestVoteReqPayload>)
            || constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                post_inflight(payload, node_id);
            }

            else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>)
            || constexpr (std::is_same_v<T, RequestVoteRespPayload>)
            || constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                post_reply(payload, node_id);
            }
            
            else if constexpr (std::is_same_v<T, ArmTimerPayload>) {
                arm_peer_timer(payload, node_id);
            }
                
            else if constexpr (std::is_same_v<T, DisarmTimerPayload>) {
                disarm_peer_timer(payload, node_id);
            }
            
            else static_assert(false, "non-exhaustive visitor!");
        }, out->data);
    });
    
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
    auto ptr = std::make_unique<RaftMessage>(std::visit([](auto&& payload) -> RpcMessage {
        return payload;
    }, req), client_id);

    while (counter < MAX_ATTEMPTS) {
        bool res = node_inbox.Push(this_id, ptr);
        ++counter;
        if (res) return true;
    }
    return false;
};

bool EventLoop::post_node_inbox(RpcReply& reply, NodeID peer_id) {
    int counter = 0;
    auto ptr = std::make_unique<RaftMessage>(visit([](auto&& payload) -> RpcMessage {
        return payload;
    }, reply), peer_id);
    
    while (counter < MAX_ATTEMPTS) {
        bool res = node_inbox.Push(this_id, ptr);
        ++counter;
        if (res) return true;
    }
    return false;
};