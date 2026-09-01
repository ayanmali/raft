#pragma once

#include "../../config.hpp"
#include "../../queues/mpsc.hpp"
#include "../conns.hpp"
#include "../../errors.hpp"
#include "../protocol/payloads.hpp"
#include "../protocol/peer.hpp"
#include "../protocol/client.hpp"
#include <asm-generic/socket.h>
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

enum EpollContextKind : uint8_t { Listen, Wake, Peer, Client, PeerTimer };

template <SocketType T>
struct ClientData;

template <>
struct ClientData<TCP> {
    std::unordered_map<IPAddrPort, ClientConn<TCP>*> client_ip_to_conn;
    std::unordered_map<FD, IPAddrPort> client_fd_to_ip;
};

template <>
struct ClientData<UDP> {
    std::unordered_map<IPAddrPort, ClientConn<UDP>*> client_ip_to_conn;
};

/*
One event loop runs on one thread.
*/
template <SocketType T>
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
    std::optional<std::string> AddPeer(NodeID id, IPAddrPort ip_addr);

    SPSCQueue<EventLoopMessage, INBOX_RING_CAP> outbound_inbox{};

    std::atomic<bool> stopped{false};

    private:
    ClientData<T> client_data;
    std::vector<PeerConn> peer_id_to_conn;

    ClientConnSlab<T> client_slab;

    NodeInbox* node_inbox; // incoming messages; multi-producer (each event loop is a producer)

    size_t this_id;
    std::atomic<bool> wake_armed{false};

    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd = -1;
    uint32_t listen_epoll_events = 0; // current epoll mask for listen_fd (used by UDP replies)
    long heartbeat_period_ms;
    long rpc_timeout_ms;

    // ---- helpers ----
    static std::optional<const char*> set_nonblocking(FD fd);
    std::optional<const char*> register_fd(FD fd, uint32_t events, EpollContextKind, TimerKind, uint32_t);
    std::optional<const char*> register_fd(FD fd, uint32_t events, EpollContextKind, uint32_t);
    std::optional<const char*> register_fd(FD fd, uint32_t events, EpollContextKind);
    std::optional<const char*> modify_client_interest(ClientConn<T>* c, uint32_t events);
    std::optional<const char*> modify_peer_interest(PeerConn& p, uint32_t events);
    std::optional<const char*> modify_listener_interest(uint32_t events);

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
    std::optional<const char*> OnClientReadable(ClientConn<TCP>* c);
    std::optional<const char*> OnClientWritable(ClientConn<TCP>* c);
    void CloseClient(ClientConn<TCP>* c);

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

template <SocketType T>
inline std::optional<std::string> EventLoop<T>::CreateEventLoop(EventLoop* loop, NodeInbox* node_inbox, size_t this_id, long heartbeat_period_ms, long rpc_timeout_ms) {
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
    std::optional<const char*> register_err = loop->register_fd(loop->listen_fd, EPOLLIN | EPOLLET, EpollContextKind::Listen);
    if (register_err) return (
        std::format("error initializing event loop; listen fd registration failed:\n{}\n", register_err.value())
    );
    loop->listen_epoll_events = EPOLLIN | EPOLLET;

    // Cross-thread event fd
    loop->event_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (loop->event_fd < 0) return ("eventfd failed");
    register_err = loop->register_fd(loop->event_fd, EPOLLIN | EPOLLET, EpollContextKind::Wake);
    if (register_err) return (
        std::format("error initializing event loop; event fd registration failed:\n{}\n", register_err.value())
    );

    return {};
}

template <SocketType T>
inline EventLoop<T>::~EventLoop() {
    if (listen_fd >= 0) ::close(listen_fd);
    if (epoll_fd >= 0) ::close(epoll_fd);
    if (event_fd >= 0) ::close(event_fd);
    for (PeerConn& p : peer_id_to_conn) {
        if (p.fd >= 0) ::close(p.fd);
    }
};

template <SocketType T>
inline void EventLoop<T>::Stop() {
    stopped.store(true, std::memory_order_release);
    wake_eventfd_unconditional();
}

template <SocketType T>
inline void EventLoop<T>::Wake() {
    #ifdef DEBUG
    std::cout << "waking event loop " << this_id << "\n";
    #endif
    if (!wake_armed.exchange(true, std::memory_order_acq_rel)) {
        wake_eventfd_unconditional();
    }

}

template <SocketType T>
inline void EventLoop<T>::wake_eventfd_unconditional() {
    #ifdef DEBUG
    std::cout << "waking event loop " << this_id << "\n";
    #endif
    uint64_t one = 1;
    ssize_t  n   = ::write(event_fd, &one, sizeof(one));
    (void)n; // EAGAIN is fine; eventfd counter is already > 0 and the loop
             // will pick up the inbox on its next wake.
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::register_fd(FD fd, uint32_t events, EpollContextKind kind, TimerKind subtype, uint32_t idx) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 |= (static_cast<uint64_t>(kind) << 56)
        | (static_cast<uint64_t>(subtype) << 48)
        | idx;

    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return ("epoll_ctl ADD failed");
    }
    return {};
}


template <SocketType T>
inline std::optional<const char*> EventLoop<T>::register_fd(FD fd, uint32_t events, EpollContextKind kind, uint32_t idx) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 |= (static_cast<uint64_t>(kind) << 56)
        | idx;

    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return ("epoll_ctl ADD failed");
    }
    return {};
}


template <SocketType T>
inline std::optional<const char*> EventLoop<T>::register_fd(FD fd, uint32_t events, EpollContextKind kind) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.u64 |= (static_cast<uint64_t>(kind) << 56)
        | fd;

    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return ("epoll_ctl ADD failed");
    }
    return {};
}

template <SocketType T>
inline std::optional<const char*> EventLoop<T>::setup_listen_socket() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    if constexpr (SOCKET_TYPE == TCP) {
        hints.ai_socktype = SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC;
    }
    if constexpr (SOCKET_TYPE == UDP) {
        hints.ai_socktype = SOCK_DGRAM | SOCK_NONBLOCK | SOCK_CLOEXEC;
    }
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    std::string port_str = std::to_string(SERVER_PORT);
    if (::getaddrinfo(nullptr, port_str.c_str(), &hints, &res) != 0 || res == nullptr) {
        return ("getaddrinfo failed");
    }

    listen_fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (listen_fd < 0) return ("socket failed");

    int yes = 1;
    // SO_REUSEPORT allows for kernel load balancing of incoming requests
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    ::setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if constexpr (SOCKET_TYPE == TCP) {
        ::setsockopt(listen_fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    }
    // TODO: tune the send/receive buffer size
    if constexpr (SOCKET_TYPE == UDP) {
        const auto send_size = RESP_SIZE + sizeof(RESP_SIZE) + sizeof(RpcKind);
        const auto rcv_size = REQ_SIZE + sizeof(REQ_SIZE) + sizeof(RpcKind);
        ::setsockopt(listen_fd, SOL_SOCKET, SO_SNDBUF, &send_size, sizeof(send_size));
        ::setsockopt(listen_fd, SOL_SOCKET, SO_RCVBUF, &rcv_size, sizeof(rcv_size));
    }

    addrinfo* p = nullptr;
    for (p = res; p; p = p->ai_next) {
        if (::bind(listen_fd, p->ai_addr, p->ai_addrlen) == 0) break;
    }

    if (!p) { ::close(listen_fd); return ("bind failed"); }

    ::freeaddrinfo(res);

    if constexpr (SOCKET_TYPE == TCP) {
        if (::listen(listen_fd, SERVER_BACKLOG) != 0) {
            ::close(listen_fd);
            return ("listen failed");
        }
    }

    return {};
}

/* Cross-thread messaging */
template <SocketType T>
inline std::optional<std::string> EventLoop<T>::DrainInbox() {
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
            using U = std::decay_t<decltype(payload)>;

            if constexpr (std::is_same_v<U, AppendEntriesReqPayload>
                || std::is_same_v<U, RequestVoteReqPayload>
                || std::is_same_v<U, InstallSnapshotReqPayload>
                || std::is_same_v<U, ForwardLeaderMsg>) {
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

            else if constexpr (std::is_same_v<U, AppendEntriesRespPayload>
                || std::is_same_v<U, RequestVoteRespPayload>
                || std::is_same_v<U, InstallSnapshotRespPayload>) {
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

            else if constexpr (std::is_same_v<U, ArmTimer>) {
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

            else if constexpr (std::is_same_v<U, DisarmTimer>) {
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

            else if constexpr (std::is_same_v<U, AddPeerMsg>) {
                #ifdef DEBUG
                std::cout << "found add peer msg\n";
                #endif

                if (!client_data.client_ip_to_conn.contains(payload.ip_addr)) {
                    return (std::format(
                        "Failed to add peer - ip address + port {} not present in client_conns\n",
                        payload.ip_addr
                    ));
                }

                std::optional<std::string> add_peer_err = AddPeer(payload.dest_id, payload.ip_addr);
                if (add_peer_err) {
                    return (std::format(
                        "Failed to add peer - AddPeer failed for ip address + port {}\n{}\n",
                        payload.ip_addr, add_peer_err.value()
                    ));
                }

            }

            else static_assert(false, "non-exhaustive visitor!");
            return {};
        }, out);
    }
    return {};
}

// drains this event loop's MPSC inbox
// for each message in the inbox, enqueue onto the corresponding peer's RPC outbox
template <SocketType T>
inline std::optional<std::string> EventLoop<T>::OnEventFd() {
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
