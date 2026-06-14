// =============================================================================
// Implementation
// =============================================================================

#include "node.hpp"
#include <csignal>
#include <random>
#ifdef DEBUG
#include <iostream>
#endif

Node::Node(NodeInbox& inbox_) : inbox(inbox_) {}

// Factory function
// Node requires stable addresses (i.e. not movable)
std::expected<std::unique_ptr<Node>, std::string> Node::CreateNode(NodeInbox& inbox) {
    static_assert(EVENT_LOOP_THREADS > 0 && (EVENT_LOOP_THREADS & (EVENT_LOOP_THREADS - 1)) == 0,
        "Node: EVENT_LOOP_THREADS must be a power of 2 (MPSC inbox requires it)");
    auto n = std::unique_ptr<Node>(new Node(inbox));

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

    // Randomized election timeout per Raft spec.
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS);
    //n->election_timeout_ = std::chrono::milliseconds(distrib(gen));
    const long election_timeout = distrib(gen);

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        std::expected<std::unique_ptr<EventLoop>, std::string> loop_raw = EventLoop::CreateEventLoop(
            MAX_SERVER_CONNS,
            inbox,
            i,
            HEARTBEAT_INTERVAL
        );
        if (!loop_raw){
            return UnexpectedF(
                std::format("error creating event loop {}:\n{}\n", i, loop_raw.error())
            );
        }
        n->loops_[i] = std::move(loop_raw.value());

        n->threads_[i] = std::thread([ptr = n.get(), i] {
            VoidExpected loop_ok = ptr->loops_[i]->Run();
            #ifdef DEBUG
            std::cout << "event loop " << i << " crashed:\n" << loop_ok.error() << "\n";
            #endif
        });
    }

    n->running_ = true;

    auto init_peers = setup_peers();
    int i{0};
    for (const auto& addr : init_peers) {
        n->node_ids.push_back(i);
        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, addr, CLIENT_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
        ++i;
    }
    n->voters.reserve(init_peers.size());
    return n;
}

Node::~Node() {
    Stop();
}

void Node::MainLoop() {
    #ifdef DEBUG
    std::cout << "starting node loop (main thread)\n";
    #endif
    while (true) {
        // check the reply inbox for new replies that have arrived
        // TODO: implement handlers
        bool leader_contact{false};
        bool demoted{false};
        inbox.DrainAll([this, &leader_contact, &demoted](std::unique_ptr<RaftMessage>&& message) {
            #ifdef DEBUG
            std::cout << "draining node inbox...\n";
            #endif
            std::visit([this, &message, &leader_contact, &demoted](auto&& payload) {
                using T = std::decay_t<decltype(payload)>;
                auto client_id = message->node_id;
                auto& el_inbox = loops_[client_id & (EVENT_LOOP_THREADS - 1)]->outbound_inbox;

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE RPC\n";
                    #endif

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
                        demoted = demoted || state == NodeState::Leader;
                        state = NodeState::Follower;
                        leader_contact = true;
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            AppendEntriesRespPayload{.success = 1}, client_id
                            )
                        );
                    }
                }

                else if constexpr (std::is_same_v<T, RequestVoteReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV RPC\n";
                    #endif

                    if (payload.term < current_term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                RequestVoteRespPayload{current_term, 0}, client_id
                            )
                        );
                        return;
                    }

                    if (payload.term > current_term) {
                        current_term = payload.term;
                        demoted = demoted || state == NodeState::Leader;
                        state = NodeState::Follower;
                        voted_for = -1;
                        leader_contact = true;
                    }

                    uint32_t last_log_term = log.back().term;
                    uint32_t last_log_idx = log.size() - 1;
                    if (payload.last_log_term > last_log_term
                    || (payload.last_log_term == last_log_term
                        && payload.last_log_idx >= last_log_idx))
                    {
                        voted_for = client_id;
                    }

                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        RequestVoteRespPayload{current_term, 1}, client_id
                        )
                    );
                }

                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC\n";
                    #endif

                    if (payload.term < current_term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            InstallSnapshotRespPayload{current_term}, client_id)
                        );
                        return;
                    }

                    current_term = payload.term;
                    demoted = demoted || state == NodeState::Leader;
                    state = NodeState::Follower;
                    voted_for = -1;
                    leader_contact = true;
                    // TODO: chunk reassembly, install snapshot to state machine.
                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        InstallSnapshotRespPayload{current_term}, client_id)
                    );
                }

                else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE reply\n";
                    #endif
                }

                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV reply\n";
                    #endif

                    if (state != NodeState::Candidate
                        || payload.term != current_term
                        || !payload.vote_granted
                        || !voters.insert(client_id).second
                    ) return;

                    // become leader if quorum of votes achieved
                    if (++votes_received >= (node_ids.size()+1) / 2) { // add 1 to account for this node
                        state = NodeState::Leader;
                        send_heartbeats_and_arm_timers();
                        voters.clear();
                        votes_received = 0;
                    }

                }

                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS reply\n";
                    #endif
                }

                // heartbeats are sent per follower, not all at once.
                else if constexpr (std::is_same_v<T, HeartbeatTimeoutPayload>) {
                    #ifdef DEBUG
                    std::cout << "found heartbeat timeout; sending heartbeats...\n";
                    #endif
                    send_rpc(AppendEntriesReqPayload{current_term}, client_id);
                    loops_[client_id & (EVENT_LOOP_THREADS - 1)]->Wake();
                }

                // These are control messages sent to event loops, not handled by Node.
                else if constexpr (std::is_same_v<T, ArmTimer> || std::is_same_v<T, DisArmTimer>) {}

                else {
                    static_assert(false, "non-exhaustive visitor");
                }
            }, message->data);
        });

        if (demoted) {
            voters.clear();
            votes_received = 0;
            voted_for = -1;
            send_disarm_timers();
            continue;
        }

        if (state == NodeState::Leader) continue;

        last_leader_contact = leader_contact
            ? std::chrono::steady_clock::now() // reset only if a leader message came in
            : last_leader_contact;

        // poll the election timer
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(last_leader_contact - now);

        if (duration < election_timeout_) continue;

        #ifdef DEBUG
        std::cout << "found election timeout...\n";
        #endif

        // start election
        if (state == NodeState::Candidate) continue;
        state = NodeState::Candidate;
        ++current_term;
        voted_for = MY_ID;
        votes_received = 1;
        last_leader_contact = std::chrono::steady_clock::now();

        for (NodeID id : node_ids) {
            send_rpc(RequestVoteReqPayload{
                current_term,
                MY_ID,
                static_cast<uint32_t>(log.size() - 1),
                log.back().term
            }, id);
            loops_[id & (EVENT_LOOP_THREADS - 1)]->Wake();
        }
    }
}

inline void Node::Stop() {
    if (!running_) return;
    running_ = false;
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (!loops_[i]->stopped.load(std::memory_order_acquire)) loops_[i]->Stop();
        loops_[i].reset();
        if (threads_[i].joinable()) threads_[i].join();
    }
}

// ---- outbound shims --------------------------------------------------------

void Node::send_rpc(AppendEntriesReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id & (EVENT_LOOP_THREADS - 1)];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
    );
}

void Node::send_rpc(RequestVoteReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id & (EVENT_LOOP_THREADS - 1)];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
    );
}

void Node::send_rpc(InstallSnapshotReqPayload&& payload, NodeID peer_id) {
    auto& el = loops_[peer_id & (EVENT_LOOP_THREADS - 1)];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(payload, peer_id)
    );
}

// runs upon winning an election
void Node::send_heartbeats_and_arm_timers() {
    for (auto id : node_ids) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        // send heartbeat rpc
        el->outbound_inbox.PushOne(
            // send heartbeat rpc
            std::make_unique<RaftMessage>(
                AppendEntriesReqPayload{current_term}, id
            )
        );
        //arm this peer's heartbeat timer so we know when to send the next heartbeat.
        el->outbound_inbox.PushOne(
            // send heartbeat rpc
            std::make_unique<RaftMessage>(
                ArmTimer{}, id
            )
        );
        el->Wake();
    }
}

/* Runs upon leader demotion */
void Node::send_disarm_timers() {
    //if (state == NodeState::Leader) return; // leader; don't run
    for (auto id : node_ids) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        el->outbound_inbox.PushOne(
            std::make_unique<RaftMessage>(
                DisArmTimer{}, id
            )
        );
        el->Wake();
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
