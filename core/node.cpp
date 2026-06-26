// =============================================================================
// Implementation
// =============================================================================

#include "node.hpp"
#include <csignal>
#include <random>
#ifdef DEBUG
#include <iostream>
#endif

Node::Node(NodeInbox& inbox_) : inbox_(inbox_) {}

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
    n->election_timeout_ = std::chrono::milliseconds(distrib(gen));
    #ifdef DEBUG
    std::cout << "election timeout set to " << n->election_timeout_ << "\n";
    #endif

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
            VoidExpectedF loop_ok = ptr->loops_[i]->Run();
            #ifdef DEBUG
            std::cout << "event loop " << i << " crashed:\n" << loop_ok.error() << "\n";
            #endif
        });
    }

    n->running_ = true;

    // init_peers is identical on every node in the cluster, so a peer's
    // index in this array IS its globally consistent NodeID. Each node lists
    // itself at index MY_ID using the placeholder "" in place of its own IP;
    // we skip that slot (the loop counter still advances so peer indices stay
    // aligned with their array positions).
    const auto init_peers = setup_peers();
    static_assert(MY_ID >= 0, "MY_ID must be non-negative");
    if (static_cast<size_t>(MY_ID) >= init_peers.size()) {
        return UnexpectedF(std::format(
            "error creating node: MY_ID {} out of range for init_peers (size {})",
            MY_ID, init_peers.size()
        ));
    }

    int i{0};
    for (auto it = init_peers.begin(); it < init_peers.begin() + MY_ID; ++it) {
        n->node_ids_.push_back(i);

        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, *it, SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
        ++i;
    }
    ++i; // to ensure IDs stay universally consistent in the cluster.
    for (auto it = init_peers.begin() + MY_ID + 1; it < init_peers.end(); ++it) {
        n->node_ids_.push_back(i);

        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, *it, SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
        ++i;
    }

    n->log_.push_back(LogEntry{});
    n->next_indexes_.insert(n->next_indexes_.end(), n->node_ids_.size(), 1);
    n->match_indexes_.resize(n->node_ids_.size());
    n->voters_.reserve(init_peers.size());
    n->last_leader_contact_ = std::chrono::steady_clock::now();
    return n;
}

Node::~Node() {
    Stop();
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

// ---- outbound --------------------------------------------------------

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

void Node::append_commands(std::vector<std::vector<std::byte>>& commands) {
    //const uint32_t last_log_idx = log_.size() - 1;
    if (state_ != NodeState::Leader) {
        #ifdef DEBUG
        std::cout << "Found append request in non-leader state - skipping\n";
        #endif
        return;
    }
    #ifdef DEBUG
    std::cout << "appending commands...\n";
    #endif
    log_.reserve(commands.size());

    #ifdef DEBUG
    std::cout << "Existing log:\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (const auto& b : e.data) {
            std::cout << static_cast<int>(b) << ", ";
        }
        std::cout << "), " << e.term << "]\n";
    }
    #endif

    for (auto& command : commands) {
        log_.emplace_back(std::move(command), current_term_);
    }

    #ifdef DEBUG
    std::cout << "New log:\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (const auto& b : e.data) {
            std::cout << static_cast<int>(b) << ", ";
        }
        std::cout << "), " << e.term << "]\n";
    }
    #endif

    // auto log_index = log_.size();


    // for (int i = 0; i < node_ids_.size(); ++i) {
    //     const uint32_t next_idx = next_index_[i];
    //     if (last_log_idx < next_idx) continue;

    //     const uint32_t prev_log_idx = next_idx > 0 ? next_idx - 1 : 0;
    //     const uint32_t prev_log_term = log_[prev_log_idx].term;

        //auto& el = loops_[node_ids_[i] & (EVENT_LOOP_THREADS - 1)];
        //auto entries_to_append = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);

        // Message will be posted to event loop when the next heartbeat is sent.

        // el->outbound_inbox.PushOne(
        //     std::make_unique<RaftMessage>(AppendEntriesReqPayload{
        //         entries_to_append,
        //         current_term_,
        //         MY_ID,
        //         prev_log_idx,
        //         prev_log_term,
        //         commit_index_
        //     }, node_ids_[i])
        // );
        //}
}

// runs upon winning an election
void Node::send_heartbeats_and_arm_timers() {
    #ifdef DEBUG
    std::cout << "Sending heartbeats and arming peer timers\n";
    #endif
    for (auto id : node_ids_) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        // send heartbeat rpc
        el->outbound_inbox.PushOne(
            std::make_unique<RaftMessage>(
                AppendEntriesReqPayload{current_term_}, id
            )
        );
        #ifdef DEBUG
        std::cout << "posted heartbeat message to peer " << id << "\n";
        #endif
        // arm this peer's heartbeat timer so we know when to send the next heartbeat.
        el->outbound_inbox.PushOne(
            // send heartbeat rpc
            std::make_unique<RaftMessage>(
                ArmTimer{}, id
            )
        );
        #ifdef DEBUG
        std::cout << "posted arm timer message to peer " << id << "\n";
        #endif
        el->Wake();
    }
}

/* Runs upon leader demotion */
void Node::send_disarm_timers() {
    //if (state == NodeState::Leader) return; // leader; don't run
    for (auto id : node_ids_) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        el->outbound_inbox.PushOne(
            std::make_unique<RaftMessage>(
                DisArmTimer{}, id
            )
        );
        el->Wake();
    }
}

void Node::demote() {
    #ifdef DEBUG
    std::cout << MY_ID << " was demoted\n";
    #endif
    voters_.clear();
    voted_for_ = -1;
    send_disarm_timers();
    state_ = NodeState::Follower;
}

void Node::become_leader() {
    #ifdef DEBUG
    std::cout << "This node (id = " << MY_ID << ") won the election\n";
    #endif
    state_ = NodeState::Leader;
    send_heartbeats_and_arm_timers();
    voters_.clear();
    voted_for_ = -1;

    const uint32_t last_log_idx = static_cast<uint32_t>(log_.size());
    for (uint32_t& i : next_indexes_) {
        i = last_log_idx + 1;
    }
    for (uint32_t& i : match_indexes_) {
        i = 0;
    }
}

void Node::add_peer_if_not_exists(NodeID node_id) {
    if (std::find(node_ids_.begin(), node_ids_.end(), node_id) != node_ids_.end()) {
        return;
    }

    node_ids_.push_back(node_id);
    next_indexes_.push_back(1);
    match_indexes_.push_back(0);

    auto& el = loops_[node_id & (EVENT_LOOP_THREADS - 1)];
    el->outbound_inbox.PushOne(
        std::make_unique<RaftMessage>(
            AddPeerMsg{}, node_id
        )
    );

}
