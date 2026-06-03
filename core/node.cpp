// =============================================================================
// Implementation
// =============================================================================

#include "node.hpp"
#include <atomic>
#include <csignal>
#include <random>
#ifdef DEBUG
#include <iostream>
#endif

Node::Node(NodeInbox& inbox_)
: inbox(inbox_) {
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
    // TODO: delete or move this
    // auto init_peers = setup_peers();
    // peers_.reserve(init_peers.size());
    // NodeID id = 0;
    // for (const auto& ip_sv : init_peers) {
    //     peers_.push_back(PeerInfo{id, ip_sv.data(), SERVER_PORT});
    //     ++id;
    // }

    // Randomized election timeout per Raft spec.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS);
    election_timeout_ = std::chrono::milliseconds(distrib(gen));

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) listen_fds_[i] = -1;
    VoidExpected sockets_ok = setup_listen_sockets();
    if (!sockets_ok) {
        #ifdef DEBUG
        std::cout << sockets_ok.error() << "\n";
        #endif
        return;
    }

    running_ = true;

    // construct every loop so all event_fds and inboxes exist
    // before any thread starts producing.
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        loops_[i] = std::make_unique<EventLoop>(
            listen_fds_[i],
            MAX_SERVER_CONNS,
            inbox,
            i);

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

VoidExpected Node::setup_listen_sockets() {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    if (::getaddrinfo(nullptr, SERVER_PORT, &hints, &res) != 0 || res == nullptr) {
        return Unexpected("getaddrinfo failed");
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        FD fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd < 0) return Unexpected("socket failed");

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
        if (!p) { ::close(fd); return Unexpected("bind failed"); }
        if (::listen(fd, SERVER_BACKLOG) != 0) {
            ::close(fd);
            return Unexpected("listen failed");
        }
        listen_fds_[i] = fd;
    }
}

void Node::main_loop() {
    #ifdef DEBUG
    std::cout << "starting main loop\n";
    #endif
    while (true) {
        // check the reply inbox for new replies that have arrived
        // TODO: implement handlers
        inbox.DrainAll([this](std::unique_ptr<RaftMessage>&& message) {
            std::visit([this, &message](auto&& payload) {
                using T = std::decay_t<decltype(payload)>;
                auto client_id = message->node_id;
                auto& el_inbox = loops_[client_id % EVENT_LOOP_THREADS]->outbound_inbox;

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    if (current_term > payload.term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                AppendEntriesRespPayload{.term = current_term, .success = 0}, client_id
                            )
                        );
                    }
                    if (current_term < payload.term) {
                        current_term = payload.term;
                        voted_for = -1;
                        leader.store(false, std::memory_order_release);
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            AppendEntriesRespPayload{}, client_id
                            )
                        );
                    }
                }

                else if constexpr (std::is_same_v<T, RequestVoteReqPayload>) {
                    if (payload.term < current_term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                RequestVoteRespPayload{current_term, 0}, client_id
                            )
                        );
                    }
                    if (payload.term > current_term) {
                        current_term = payload.term;
                        leader.store(false, std::memory_order_release);
                    }

                    uint8_t granted = 0;
                    // if (voted_for == req.candidate_id) {
                    //     // TODO: log up-to-date check (last_log_idx/last_log_term).
                    //     voted_for = req.candidate_id;
                    //     granted   = 1;
                    // }

                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        RequestVoteRespPayload{current_term, granted}, client_id
                        )
                    );
                }

                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    if (payload.term < current_term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            InstallSnapshotRespPayload{current_term}, client_id)
                        );
                    }

                    if (payload.term > current_term) {
                        current_term = payload.term;
                        leader.store(false, std::memory_order_release);
                    }

                    // TODO: chunk reassembly, install snapshot to state machine.
                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        InstallSnapshotRespPayload{current_term}, client_id)
                    );
                }

                else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>) {

                }
                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {

                }
                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {

                }
                else if constexpr (std::is_same_v<T, HeartbeatTimeoutPayload>) {
                    if (leader.load(std::memory_order_acquire)) {
                        send_rpc(AppendEntriesReqPayload{current_term}, client_id);
                    }
                }
                else if constexpr (std::is_same_v<T, ArmTimerPayload> || std::is_same_v<T, DisarmTimerPayload>) {
                    // These are control messages sent to event loops, not handled by Node.
                }
                else {
                    static_assert(false, "non-exhaustive visitor");
                }
            }, message->data);
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
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
    );
}

void Node::send_rpc(RequestVoteReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id % EVENT_LOOP_THREADS];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
    );
}

void Node::send_rpc(InstallSnapshotReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id % EVENT_LOOP_THREADS];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
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
            el->outbound_inbox.PushOne(
                std::make_unique<RaftMessage>(
                    ArmTimerPayload{
                        .period = HEARTBEAT_INTERVAL
                    }, id
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
            el->outbound_inbox.PushOne(
                std::make_unique<RaftMessage>(
                    DisarmTimerPayload{}, id
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
