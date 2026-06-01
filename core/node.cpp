// =============================================================================
// Implementation
// =============================================================================

#include "node.hpp"
#include <csignal>
#include <random>

Node::Node(NodeRequestInbox& request_inbox_, NodeReplyInbox& reply_inbox_)
: request_inbox(request_inbox_), reply_inbox{reply_inbox_} {
    static_assert(EVENT_LOOP_THREADS > 0 && (EVENT_LOOP_THREADS & (EVENT_LOOP_THREADS - 1)) == 0,
                  "Node: EVENT_LOOP_THREADS must be a power of 2 (MPSC inbox requires it)");

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

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) listen_fds_[i] = -1;
    setup_listen_sockets();

    running_ = true;

    // construct every loop so all event_fds and inboxes exist
    // before any thread starts producing.
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        loops_[i] = std::make_unique<EventLoop>(
            listen_fds_[i],
            MAX_SERVER_CONNS,
            request_inbox,
        reply_inbox);
    }

    // spawn the worker threads.
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        threads_[i] = std::thread([this, i] { loops_[i]->Run(); });
    }
}

Node::~Node() {
    stop();
    for (auto& fd : listen_fds_) {
        if (fd >= 0) { ::close(fd); fd = -1; }
    }
}

void Node::setup_listen_sockets() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    if (::getaddrinfo(nullptr, SERVER_PORT, &hints, &res) != 0 || res == nullptr) {
        throw std::runtime_error("getaddrinfo failed");
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
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

void Node::main_loop() {
    while (true) {
        // check the reply inbox for new replies that have arrived
        // TODO: implement handlers
        reply_inbox.DrainAll([this](RpcReply&& reply){
            handle_reply(reply);
        });

        request_inbox.DrainAll([this](RpcRequest&& req){
            std::expected<RpcReply, const char*> reply_raw = handle_request(req);
            if (!reply_raw) {
                // TODO: handle error
            }
        });

        
    }
}

inline void Node::stop() {
    if (!running_) return;
    running_ = false;
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (loops_[i]) loops_[i]->Stop();
    }
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (threads_[i].joinable()) threads_[i].join();
    }
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) loops_[i].reset();
}

// ---- outbound shims --------------------------------------------------------

void Node::send_rpc(AppendEntriesReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id % EVENT_LOOP_THREADS];
    el->outbound_inbox.EmplaceOne(
        std::make_unique<Outbound>(payload, peer_id)
    );
}

void Node::send_rpc(RequestVoteReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id % EVENT_LOOP_THREADS];
    el->outbound_inbox.EmplaceOne(
        std::make_unique<Outbound>(payload, peer_id)
    );
}

void Node::send_rpc(InstallSnapshotReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id % EVENT_LOOP_THREADS];
    el->outbound_inbox.EmplaceOne(
        std::make_unique<Outbound>(payload, peer_id)
    );
}

void Node::send_heartbeats() {
    if (!leader.load(std::memory_order_acquire)) return; // not leader; nothing to send
    // for each event loop, call send_append_entries_rpc() on each peer
    for (auto& el : loops_) {
        for (auto& [id, conn] : el->peer_conns) {
            send_rpc(AppendEntriesReqPayload{current_term}, id);
        }
    }
}

/* Runs upon winning an election */
void Node::send_arm_timers() {
    if (!leader.load(std::memory_order_acquire)) return; // not leader; don't send
    for (auto& el : loops_) {
        for (auto& [id, conn] : el->peer_conns) {
            auto& el = loops_[id % EVENT_LOOP_THREADS];
            el->outbound_inbox.EmplaceOne(
                std::make_unique<Outbound>(
                    ArmTimerPayload{
                        .period = HEARTBEAT_INTERVAL
                    },
                    id
                )
            );
        }
    }
}

/* Runs upon leader demotion */
inline void Node::send_disarm_timers() {
    if (leader.load(std::memory_order_acquire)) return; // leader; don't run
    for (auto& el : loops_) {
        for (auto& [id, conn] : el->peer_conns) {
            auto& el = loops_[id % EVENT_LOOP_THREADS];
            el->outbound_inbox.EmplaceOne(
                std::make_unique<Outbound>(
                    DisarmTimerPayload{},
                    id
                )
            );
        }
    }
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
