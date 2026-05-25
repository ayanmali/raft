#pragma once
/*
Raft node.

Concurrency model:
  - N is the number of worker threads. Each thread owns a self-contained
    EventLoop:
      * its own listening socket bound to SERVER_PORT via SO_REUSEPORT
        (the kernel hashes incoming connection 4-tuples to one queue,
        so each thread sees a disjoint set of inbound clients);
      * its own eventfd for cross-thread wakeups;
      * its own epoll instance covering listen fd, eventfd, accepted
        client fds, and the loop's slice of peer fds.
  - Peers are sharded across loops by `peer_id % N`. Outbound RPCs to
    peer p are always sent from loop p % N; inbound replies for that
    peer arrive on the same loop. No cross-thread peer state.
  - Raft state (currentTerm, votedFor, log, commitIndex, lastApplied)
    is held on Node and protected by `state_mu_`. Inbound RPC handlers
    and outbound reply handlers all run on event-loop threads and
    acquire the mutex.

Persistence:
  On disk:
    - currentTerm
    - votedFor
    - log[]
  Volatile (all servers):
    - commitIndex
    - lastApplied
  Volatile (leaders, reinitialized after election):
    - nextIndex[]
    - matchIndex[]
*/
#include "../config.hpp"
#include "../rpc/conns.hpp"
#include "../rpc/event_loop_v2.hpp"
#include "../rpc/protocol/payloads.hpp"
#include "../rpc/protocol/utils.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <random>
#include <signal.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

struct LogEntry {
    std::vector<std::byte> data;
    int term;
};

/*
N - the number of threads running an event loop
*/
template <uint N>
struct Node {
public:
    Node(NodeReplyInbox<N>& reply_inbox);
    ~Node();

    Node(const Node&)            = delete;
    Node& operator=(const Node&) = delete;

    // Spawns N worker threads, each running an EventLoop. Returns
    // immediately; threads run until stop() or the dtor.
    // main thread runs in a loop, waiting for election timeout
    void start();

    // Signals every loop to exit, joins all worker threads.
    void stop();

    void main_loop();

    // Outbound RPC entry points. Resolve the owning loop by
    // peer_id % N and post the request into its inbox. The reply
    // callback fires on that loop's thread.
    void send_rpc(AppendEntriesReqPayload&& payload, NodeID peer_id);
    void send_rpc(RequestVoteReqPayload&& payload, NodeID peer_id);
    void send_rpc(InstallSnapshotReqPayload&& payload, NodeID peer_id);
    void send_heartbeats();

    void send_arm_timers(); // upon leader promotion
    void send_disarm_timers(); // upon leader demotion
    void post_loop_inbox(std::unique_ptr<Outbound>);

    // Raft leadership transitions. Both must run on an event-loop thread
    // (i.e. as part of a state-machine reaction to an inbound RPC, reply,
    // or timer fire) so g_loop_producer_id is set and the Enqueue* path
    // routes correctly. Snapshot the decision under state_mu_ in the
    // caller, release it, and then call these.
    //
    // Each peer's heartbeat timer is armed on the loop that owns that
    // peer (peer.id % N), via an inbox-routed control message. The
    // owning loop performs timerfd_settime; no cross-loop syscalls.
    // void on_leader_elected();
    // void on_leader_demoted();

    // Inbound handlers. Locked under state_mu_; called by event loops
    // when they finish parsing one full request frame from a client.

    // RpcReply handle_request(const RpcMessage& message);

    // AppendEntriesRespPayload     handle_append_entries(const AppendEntriesReqPayload&);
    // RequestVoteRespPayload       handle_request_vote(const RequestVoteReqPayload&);
    // InstallSnapshotRespPayload   handle_install_snapshot(const InstallSnapshotReqPayload&);
    std::expected<RpcReply, const char*> handle_request(const RpcMessage& req);
    void handle_reply(const RpcReply& reply);
    
    void handle_reply(const AppendEntriesRespPayload& reply);
    void handle_reply(const RequestVoteRespPayload& reply);
    void handle_reply(const InstallSnapshotRespPayload& reply);

    void handle_append_entries_reply(const RpcReply& reply);
    void handle_request_vote_reply(const RpcReply& reply);
    void handle_install_snapshot_reply(const RpcReply& reply);

private:
    // ---- transport ----
    std::array<FD, N>                              listen_fds_{};
    std::array<std::unique_ptr<EventLoop<N>>, N>   loops_;
    std::array<std::thread, N>                     threads_;
    bool                                           running_ = false;

    NodeReplyInbox<N>& reply_inbox;
    NodeRequestInbox<N>& request_inbox;

    // ---- raft state ----
    std::mutex                                     state_mu_;              // since multiple event loop threads could modify state concurrently
    uint32_t                                       current_term  = 0;
    uint32_t                                       voted_for     = -1;
    std::vector<LogEntry>                          log;
    uint32_t                                       commit_index  = 0;
    uint32_t                                       last_applied  = 0;
    // std::vector<int>                            next_index;             // leader-only, per peer
    // std::vector<int>                            match_index;            // leader-only, per peer
    std::atomic<bool>                              leader        = false;

    // Election timeout, randomized at construction.
    std::chrono::milliseconds election_timeout_;

    std::vector<PeerInfo> peers_;

    // ---- setup helpers ----

    // Creates N listening sockets, all bound to SERVER_PORT via
    // SO_REUSEPORT. Sockets are non-blocking, CLOEXEC, TCP_NODELAY.
    void setup_listen_sockets();

    // Builds the peer subset for thread `i` (peers where peer_id % N == i).
    auto peer_subset_for(uint i) const;

    // Constructs the RpcHandlers struct (member-fn lambdas) the loops
    // dispatch to on inbound requests.
    // RpcHandlers make_handlers();

    void tick_peer(NodeID peer_id);

    // std::expected<RpcReply, const char*> handle_append_entries_req(const RpcMessage& message);
    // std::expected<RpcReply, const char*> handle_request_vote_req(const RpcMessage& message);
    // std::expected<RpcReply, const char*> handle_install_snapshot_req(const RpcMessage& message);

};

// =============================================================================
// Implementation
// =============================================================================

template <uint N>
inline Node<N>::Node(NodeReplyInbox<N>& reply_inbox_)
: reply_inbox{reply_inbox_} {
    static_assert(N > 0 && (N & (N - 1)) == 0,
                  "Node<N>: N must be a power of 2 (MPSC inbox requires it)");

    // SIGPIPE would otherwise kill the process if a peer disappears
    // mid-send. send/recv calls also pass MSG_NOSIGNAL belt-and-
    // suspenders.
    static const auto sigpipe_ignored = [] {
        struct sigaction sa{};
        sa.sa_handler = SIG_IGN;
        sigemptyset(&sa.sa_mask);
        ::sigaction(SIGPIPE, &sa, nullptr);
        return true;
    }();
    (void)sigpipe_ignored;

    // Build peer table. setup_peers() returns null-terminated string
    // literals (constexpr static storage), so .data() pointers stay
    // valid for the lifetime of the process.
    auto init_peers = setup_peers();
    peers_.reserve(init_peers.size());
    NodeID id = 0;
    for (const auto& ip_sv : init_peers) {
        peers_.push_back(PeerInfo{id, ip_sv.data(), SERVER_PORT});
        ++id;
    }

    // Randomized election timeout per Raft spec.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS);
    election_timeout_ = std::chrono::milliseconds(distrib(gen));

    for (uint i = 0; i < N; ++i) listen_fds_[i] = -1;
    setup_listen_sockets();
}

template <uint N>
inline Node<N>::~Node() {
    stop();
    for (auto& fd : listen_fds_) {
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
}

template <uint N>
inline void Node<N>::setup_listen_sockets() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    if (::getaddrinfo(nullptr, SERVER_PORT, &hints, &res) != 0 || res == nullptr) {
        throw std::runtime_error("getaddrinfo failed");
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    for (uint i = 0; i < N; ++i) {
        FD fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) throw std::runtime_error("socket failed");

        int yes = 1;
        // SO_REUSEPORT must be set BEFORE bind() so that all N sockets
        // bound to the same port are members of the same SO_REUSEPORT
        // group; the kernel then load-balances incoming connections
        // across them.
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        addrinfo* p = nullptr;
        for (p = res; p; p = p->ai_next) {
            if (::bind(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        }
        if (!p) { ::close(fd); throw std::runtime_error("bind failed"); }
        if (::listen(fd, SERVER_BACKLOG) != 0) {
            ::close(fd);
            throw std::runtime_error("listen failed");
        }
        listen_fds_[i] = fd;
    }
}

// template <uint N>
// inline auto Node<N>::peer_subset_for(uint i) const {
//     auto subset = peers_ | std::views::filter([i](PeerInfo& pi) { return pi.id % N == i; });
//     return subset;
// }

template <uint N>
inline void Node<N>::start() {
    if (running_) return;
    running_ = true;

    // Phase 1: construct every loop so all event_fds and inboxes exist
    // before any thread starts producing.
    std::array<EventLoop<N>*, N> raw{};
    for (uint i = 0; i < N; ++i) {
        loops_[i] = std::make_unique<EventLoop<N>>(
            listen_fds_[i],
            peer_subset_for(i),
            make_handlers(),
            MAX_SERVER_CONNS,
            static_cast<size_t>(i));
        raw[i] = loops_[i].get();
    }
    // Phase 2: wire sibling pointers. Necessary before any loop thread
    // can Enqueue* into another loop's inbox.
    for (uint i = 0; i < N; ++i) loops_[i]->set_loops(raw);

    // Phase 3: spawn the worker threads. Each one sets g_loop_producer_id
    // = my_id_ at the top of Run() before pushing into any sibling inbox.
    for (uint i = 0; i < N; ++i) {
        threads_[i] = std::thread([this, i] { loops_[i]->Run(); });
    }

    // if timeout occurs, start an election
    // ...
}

template <uint N>
inline void Node<N>::main_loop() {
    while (true) {
        // check the reply inbox for new replies that have arrived
        // TODO: implement handlers
        reply_inbox.DrainAll([](RpcReply&& reply){
            handle_reply<N>(reply);
        });

        request_inbox.DrainAll([](RpcMessage&& req){
            handle_request<N>(req);
        });

        
    }
}

template <uint N>
inline void Node<N>::stop() {
    if (!running_) return;
    running_ = false;
    for (uint i = 0; i < N; ++i) {
        if (loops_[i]) loops_[i]->Stop();
    }
    for (uint i = 0; i < N; ++i) {
        if (threads_[i].joinable()) threads_[i].join();
    }
    for (uint i = 0; i < N; ++i) loops_[i].reset();
}

// ---- outbound shims --------------------------------------------------------

template <uint N>
inline void Node<N>::send_rpc(
    AppendEntriesReqPayload&& payload, NodeID peer_id) {
    auto outbound = std::make_unique<Outbound>(Outbound{.data = payload, .peer_id = peer_id});
    post_loop_inbox(std::move(outbound));
}

template <uint N>
inline void Node<N>::send_rpc(RequestVoteReqPayload&& payload, NodeID peer_id) {
    auto outbound = std::make_unique<Outbound>(Outbound{.data = payload, .peer_id = peer_id});
    post_loop_inbox(std::move(outbound));
}

template <uint N>
inline void Node<N>::send_rpc(
    InstallSnapshotReqPayload&& payload, NodeID peer_id) {
    auto outbound = std::make_unique<Outbound>(Outbound{.data = payload, .peer_id = peer_id});
    post_loop_inbox(std::move(outbound));
}

template <uint N>
inline void Node<N>::send_heartbeats() {
    if (!leader.load(std::memory_order_acquire)) return; // not leader; nothing to send
    // for each event loop, call send_append_entries_rpc() on each peer
    for (auto& el : loops_) {
        for (auto& p : el->peer_conns) {
            send_append_entries_rpc(p.id, AppendEntriesReqPayload{current_term});
        }
    }
}

/* Runs upon winning an election */
template <uint N>
inline void Node<N>::send_arm_timers() {
    if (!leader.load(std::memory_order_acquire)) return; // not leader; don't send
    for (auto& el : loops_) {
        for (auto& p : el->peer_conns) {
            auto out = std::make_unique<Outbound>
                (Outbound{
                    .data = ArmTimerPayload{.period = HEARTBEAT_INTERVAL},
                    .peer_id = p.peer_id
                });
            post_loop_inbox(out);
        }
    }
}

template <uint N>
/* Runs upon leader demotion */
inline void Node<N>::send_disarm_timers() {
    if (leader.load(std::memory_order_acquire)) return; // leader; don't run
    for (auto& el : loops_) {
        for (auto& p : el->peer_conns) {
            auto out = std::make_unique<Outbound>
                (Outbound{
                    .data = DisarmTimerPayload{},
                    .peer_id = p.peer_id
                });
            post_loop_inbox(out);
        }
    }
}

template <uint N>
inline void Node<N>::post_loop_inbox(std::unique_ptr<Outbound> out) {
    NodeID peer_id = out->peer_id;
    auto l = loops_[peer_id % N];
    if (!l) throw std::runtime_error("Node not started");
    EventLoop<N>* loop = l.get();
    loop->inbox_.Push(peer_id, std::move(out));
}

// template <uint N>
// void Node<N>::tick_peer(NodeID peer_id) {
//     AppendEntriesReqPayload payload{}; // heartbeat message == empty AE message
//     // {
//     //     std::lock_guard<std::mutex> lk(state_mu_);
//     //     if (!leader) return;                                      // not leader: nothing to send
//     // }
//     if (!leader.load(std::memory_order_acquire)) return; // not leader; nothing to send
//     // Enqueue lands in this peer's owning loop's inbox.
//     // g_loop_producer_id is set because we're on a loop thread.
//     // loops_[peer_id % N]->EnqueueAE(
//     //     peer_id, std::move(payload),
//     //     [this, peer_id](AppendEntriesRespPayload r) { on_ae_reply(peer_id, r); });

// }

// template <uint N>
// inline void Node<N>::on_leader_elected() {
//     // Caller has already snapshotted state under state_mu_ and decided
//     // we're now leader. Arm every peer's heartbeat timer on its owning
//     // loop. The Enqueue path routes through the inbox so the
//     // timerfd_settime syscall happens on the owning loop's thread, not
//     // on whichever loop detected the election win.
//     // for (const auto& p : peers_) {
//     //     loops_[p.id % N]->EnqueueArmTimer(p.id, HEARTBEAT_INTERVAL);
//     // }
//     for (auto& loop : loops_) {
//         for (auto& [id, pc] : loop->peer_conns) {
//             loop->EnqueueArmTimer(id, HEARTBEAT_INTERVAL);
//         }
//     }
// }

// template <uint N>
// inline void Node<N>::on_leader_demoted() {
//     // for (const auto& p : peers_) {
//     //     loops_[p.id % N]->EnqueueDisarmTimer(p.id);
//     // }
//     for (auto& loop : loops_) {
//         for (auto& [id, pc] : loop->peer_conns) {
//             loop->EnqueueDisarmTimer(id, HEARTBEAT_INTERVAL);
//         }
//     }
// }
