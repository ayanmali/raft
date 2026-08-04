#pragma once

#include "../../config.hpp"
#include "../../queues/mpsc.hpp"
#include "../conns.hpp"
#include "../../errors.hpp"
#include "../protocol/payloads.hpp"
#include "../protocol/peer.hpp"
#include "../protocol/client.hpp"
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <format>
#include <unordered_map>
#ifdef DEBUG
#include <iostream>
#endif
#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/timerfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

constexpr int MAX_ATTEMPTS = 10;

constexpr size_t INBOX_RING_CAP = 1024; // per producer; must be power of 2

// for processing incoming requests/replies
using NodeInbox = MPSC<NodeMessage, INBOX_RING_CAP, EVENT_LOOP_THREADS>;

struct ReplyHandlerVisitor;
struct RequestHandlerVisitor;

/*
One event loop runs on one thread.
*/
struct EventLoop {
    public:
    static std::optional<std::string> CreateEventLoop(EventLoop*, NodeInbox*, size_t this_id, long heartbeat_period_ms, long rpc_timeout_ms);
    EventLoop(size_t inbound_cap, NodeInbox*, size_t this_id, long period);
    EventLoop() = default;
    ~EventLoop();
    EventLoop(EventLoop&&) = delete;
    EventLoop& operator=(EventLoop&&) = delete;
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    std::optional<std::string> Run();
    void Stop(); // event loop can be stopped via a signal on the event fd
    void Wake();
    std::optional<std::string> AddPeer(NodeID id, const char* ip_addr, const char* port);

    SPSCQueue<EventLoopMessage, INBOX_RING_CAP> outbound_inbox{};

    std::atomic<bool> stopped{false};

    private:
    ClientConnSlab client_slab;
    std::unordered_map<NodeID, PeerConn> peer_conns;
    std::unordered_map<FD, ClientConn*> client_conns;
    std::atomic<bool> wake_armed{false};

    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd = -1;

    // Inbound
    NodeInbox* node_inbox; // incoming messages; multi-producer (each event loop is a producer)

    // ClientID next_conn_id = 0;

    size_t this_id;

    // Outbound

    std::unordered_map<FD, NodeID> peer_fd_to_id;
    std::unordered_map<FD, std::pair<NodeID, TimerKind>> peer_timer_fd_map; // heartbeats and RPC timeouts

    long heartbeat_period_ms;
    long rpc_timeout_ms;

    // ---- helpers ----
    static std::optional<const char*> set_nonblocking(FD fd);
    std::optional<const char*> register_fd(FD fd, uint32_t events);
    std::optional<const char*> modify_client_interest(ClientConn* c, uint32_t events);
    std::optional<const char*> modify_peer_interest(PeerConn& p, uint32_t events);

    std::optional<std::string> post_inflight(AppendEntriesReqPayload& payload);
    std::optional<std::string> post_inflight(RequestVoteReqPayload& payload);
    std::optional<std::string> post_inflight(InstallSnapshotReqPayload& payload);
    std::optional<std::string> post_inflight(ForwardLeaderMsg& payload);

    std::optional<std::string> post_reply(AppendEntriesRespPayload& payload);
    std::optional<std::string> post_reply(RequestVoteRespPayload& payload);
    std::optional<std::string> post_reply(InstallSnapshotRespPayload& payload);

    // inbound messaging
    std::optional<const char*> setup_listen_socket();
    std::optional<const char*> Accept();
    std::optional<const char*> OnClientReadable(ClientConn* c);
    std::optional<const char*> OnClientWritable(ClientConn* c);
    void CloseClient(ClientConn* c);

    // outbound messaging
    std::optional<const char*> OnPeerWritable(PeerConn& p);
    std::optional<const char*> OnPeerReadable(PeerConn& p);
    std::optional<const char*> OnPeerHeartbeatTimeout(PeerConn& p);
    std::optional<const char*> OnPeerAERPCTimeout(PeerConn& p);
    std::optional<const char*> OnPeerRVRPCTimeout(PeerConn& p);
    std::optional<const char*> OnPeerISRPCTimeout(PeerConn& p);
    std::optional<std::string> StartConnect(PeerConn& p);
    void DropPeer(PeerConn& p);

    // wake / inbox
    std::optional<std::string> arm_heartbeat_timer(NodeID peer_id);
    std::optional<std::string> disarm_heartbeat_timer(NodeID peer_id);
    std::optional<std::string> DrainInbox();
    std::optional<std::string> OnEventFd();
    void wake_eventfd_unconditional();

    bool post_node_inbox(NodeMessage&& msg) {
        #ifdef DEBUG
        std::cout << "Posting message to node inbox\n";
        #endif
        for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
            if (node_inbox->Push(this_id,
                NodeMessage(std::forward<NodeMessage>(msg)))) {
                return true;
            }
        }
        return false;
    }
};

#include "./client.hpp"
#include "./peer.hpp"

inline std::optional<std::string> EventLoop::CreateEventLoop(EventLoop* loop, NodeInbox* node_inbox, size_t this_id, long heartbeat_period_ms, long rpc_timeout_ms) {
    loop->node_inbox = node_inbox;
    loop->this_id = this_id;
    loop->heartbeat_period_ms = heartbeat_period_ms;
    loop->rpc_timeout_ms = rpc_timeout_ms;

    // Epoll fd
    loop->epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
    if (loop->epoll_fd < 0) return ("epoll_create1 failed");

    // Listening socket
    std::optional<const char*> listen_err = loop->setup_listen_socket();
    if (listen_err) return ("listen socket could not be set up");
    std::optional<const char*> non_blocking_err = loop->set_nonblocking(loop->listen_fd);
    if (non_blocking_err) return (
        std::format("error initializing event loop:\n{}\n", non_blocking_err.value())
    );
    std::optional<const char*> register_err = loop->register_fd(loop->listen_fd, EPOLLIN | EPOLLET);
    if (register_err) return (
        std::format("error initializing event loop; listen fd registration failed:\n{}\n", register_err.value())
    );

    // Cross-thread event fd
    loop->event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (loop->event_fd < 0) return ("eventfd failed");
    register_err = loop->register_fd(loop->event_fd, EPOLLIN | EPOLLET);
    if (register_err) return (
        std::format("error initializing event loop; event fd registration failed:\n{}\n", register_err.value())
    );

    return {};
}

inline EventLoop::~EventLoop() {
    if (listen_fd >= 0) ::close(listen_fd);
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    // Per-peer fds: close anything still live.
    for (auto& [id, p] : peer_conns) {
        if (p.fd >= 0) ::close(p.fd);
    }

};

inline void EventLoop::Stop() {
    stopped.store(true, std::memory_order_release);
    wake_eventfd_unconditional();
}

inline void EventLoop::Wake() {
    #ifdef DEBUG
    std::cout << "waking event loop " << this_id << "\n";
    #endif
    if (!wake_armed.exchange(true, std::memory_order_acq_rel)) {
        wake_eventfd_unconditional();
    }

}

inline void EventLoop::wake_eventfd_unconditional() {
    #ifdef DEBUG
    std::cout << "waking event loop " << this_id << "\n";
    #endif
    uint64_t one = 1;
    ssize_t  n   = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

inline std::optional<const char*> EventLoop::set_nonblocking(FD fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) return ("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        return ("fcntl F_SETFL O_NONBLOCK failed");
    }
    return {};
}

inline std::optional<const char*> EventLoop::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return ("epoll_ctl ADD failed");
    }
    return {};
}

inline std::optional<const char*> EventLoop::setup_listen_socket() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    if (::getaddrinfo(nullptr, SERVER_PORT, &hints, &res) != 0 || res == nullptr) {
        return ("getaddrinfo failed");
    }

    listen_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) return ("socket failed");

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
    if (!p) { ::close(listen_fd); return ("bind failed"); }
    if (::listen(listen_fd, SERVER_BACKLOG) != 0) {
        ::close(listen_fd);
        return ("listen failed");
    }
    ::freeaddrinfo(res);
    return {};
}

inline std::optional<std::string> EventLoop::Run() {
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
            return ("epoll_wait failed");
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
                std::optional<const char*> accept_err = Accept();
                if (accept_err) std::cout << "failed to accept new client connection; skipping:\n" << accept_err.value() << "\n";
                continue;
            }
            if (fd == event_fd) {
                #ifdef DEBUG
                std::cout << "event fd awakened\n";
                #endif
                std::optional<std::string> on_event_fd_err = OnEventFd();
                if (on_event_fd_err) {
                    return (std::format(
                        "Failed to process new event:\n{}\n",
                        on_event_fd_err.value()
                    ));
                }
                continue;
            }

            if (auto it = client_conns.find(fd); it != client_conns.end()) {
                ClientConn* c = it->second;
                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    #ifdef DEBUG
                    std::cout << "epoll error found for client " << it->second->client_ip_addr << ":";
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
                    std::cout << "new client message from client with ip " << c->client_ip_addr << "\n";
                    #endif
                    std::optional<const char*> readable_err = OnClientReadable(c);
                    if (readable_err) {
                        #ifdef DEBUG
                        std::cout << "failed to read incoming client message from client with ip " << c->client_ip_addr << ":\n" << readable_err.value() << "\n";
                        #endif
                        continue;
                    }
                }
                if (e & EPOLLOUT) {
                    #ifdef DEBUG
                    std::cout << "ready to send reply to client with ip " << c->client_ip_addr << "\n";
                    #endif
                    std::optional<const char*> writable_err = OnClientWritable(c);
                    if (writable_err) {
                        #ifdef DEBUG
                        std::cout << "failed to write to client socket:\n" << writable_err.value() << "\n";
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
                    std::optional<const char*> readable_err = OnPeerReadable(p);
                    if (readable_err) {
                        #ifdef DEBUG
                        std::cout << "failed to read incoming peer reply:\n" << readable_err.value() << "\n";
                        #endif
                        continue;
                    }
                }
                if (e & EPOLLOUT) {
                    #ifdef DEBUG
                    std::cout << "ready to send RPC to peer " << p.peer_id << "\n";
                    #endif
                    std::optional<const char*> writable_err = OnPeerWritable(p);
                    if (writable_err) {
                        #ifdef DEBUG
                        std::cout << "failed to write RPC to peer socket:\n" << writable_err.value() << "\n";
                        #endif
                        continue;
                    }
                }
                continue;
            }

            if (auto it = peer_timer_fd_map.find(fd); it != peer_timer_fd_map.end()) {
                auto [peer_id, kind] = it->second;
                PeerConn& p = peer_conns.at(peer_id);
                if (e & EPOLLIN) {
                    #ifdef DEBUG
                    const char* kind_name[] = {"heartbeat", "AE", "RV", "IS"};
                    std::cout << kind_name[static_cast<uint8_t>(kind)] << " timer fired for peer " << p.peer_id << "\n";
                    #endif
                    std::optional<const char*> timer_err;
                    switch (kind) {
                        case TimerKind::Heartbeat: timer_err = OnPeerHeartbeatTimeout(p); break;
                        case TimerKind::AE:        timer_err = OnPeerAERPCTimeout(p); break;
                        case TimerKind::RV:        timer_err = OnPeerRVRPCTimeout(p); break;
                        case TimerKind::IS:        timer_err = OnPeerISRPCTimeout(p); break;
                    }
                    if (timer_err) {
                        #ifdef DEBUG
                        std::cout << "failed to handle " << kind_name[static_cast<uint8_t>(kind)]
                                  << " timer for peer " << p.peer_id << ":\n" << timer_err.value() << "\n";
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

inline std::optional<std::string> EventLoop::DrainInbox() {
    // Disarm BEFORE draining the ring. Any producer that pushes after this
    // store but before we finish draining will see armed=false, rearm, and
    // re-wake -- so the next epoll_wait will see the eventfd already
    // counted up and we'll come right back. No lost items.
    #ifdef DEBUG
    std::cout << "draining inbox...\n";
    #endif
    wake_armed.store(false, std::memory_order_release);

    EventLoopMessage out;
    while (outbound_inbox.PopOne(&out)) {
        std::optional<std::string> ok = std::visit([&out, this](auto&& payload) -> std::optional<std::string> {
            using T = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<T, AppendEntriesReqPayload>
                || std::is_same_v<T, RequestVoteReqPayload>
                || std::is_same_v<T, InstallSnapshotReqPayload>
                || std::is_same_v<T, ForwardLeaderMsg>) {
                #ifdef DEBUG
                std::cout << "found request in event loop outbound inbox\n";
                #endif
                std::optional<std::string> post_err = post_inflight(payload);
                if (post_err) {
                    return (std::format(
                        "Error while draining event loop inbox - failed to post RPC to inflight queue for peer {}:\n{}",
                        payload.dest_id, post_err.value()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>
                || std::is_same_v<T, RequestVoteRespPayload>
                || std::is_same_v<T, InstallSnapshotRespPayload>) {
                #ifdef DEBUG
                std::cout << "found reply in event loop outbound inbox\n";
                #endif
                std::optional<std::string> post_err = post_reply(payload);
                if (post_err) {
                    return (std::format(
                        "Error while draining event loop inbox - failed to post reply to inflight queue for peer {}:\n{}",
                        payload.server_id, post_err.value()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, ArmTimer>) {
                #ifdef DEBUG
                std::cout << "found arm timer req\n";
                #endif
                std::optional<std::string> arm_err = arm_heartbeat_timer(payload.dest_id);
                if (arm_err) {
                    return (std::format(
                        "Error while draining inbox: failed to arm timers for node {}:\n{}\n",
                        payload.dest_id, arm_err.value()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, DisarmTimer>) {
                #ifdef DEBUG
                std::cout << "found disarm timer req\n";
                #endif
                std::optional<std::string> disarm_err = disarm_heartbeat_timer(payload.dest_id);
                if (disarm_err) {
                    return (std::format(
                        "Error while draining inbox: failed to disarm timers for node {}:\n{}\n",
                        payload.dest_id, disarm_err.value()
                    ));
                }
            }

            else if constexpr (std::is_same_v<T, AddPeerMsg>) {
                #ifdef DEBUG
                std::cout << "found add peer msg\n";
                #endif

                if (auto it = client_conns.find(payload.fd); it != client_conns.end()) {
                    std::optional<std::string> add_peer_err = AddPeer(payload.dest_id, it->second->client_ip_addr, payload.port);
                    if (add_peer_err) {
                        return (std::format(
                            "Failed to add peer - AddPeer failed for fd {}\n{}\n",
                            payload.fd, add_peer_err.value()
                        ));
                    }
                }

                return (std::format(
                    "Failed to add peer - fd {} not present in client_conns\n",
                    payload.fd
                ));
            }

            else static_assert(false, "non-exhaustive visitor!");
            return {};
        }, out);
    }
    return {};
}

// drains this event loop's MPSC inbox
// for each message in the inbox, enqueue onto the corresponding peer's RPC outbox
inline std::optional<std::string> EventLoop::OnEventFd() {
    uint64_t counter;
    for (;;) {
        ssize_t n = ::read(event_fd, &counter, sizeof(counter));
        if (n == sizeof(counter)) break;
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        break;
    }
    std::optional<std::string> drain_err = DrainInbox();
    if (drain_err) {
        return (std::format(
            "Error while draining inbox:\n{}\n",
            drain_err.value()
        ));
    }
    return {};
}
