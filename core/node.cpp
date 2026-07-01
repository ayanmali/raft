// =============================================================================
// Implementation
// =============================================================================

#include "node.hpp"
#include <csignal>
#include <random>
#include <netinet/in.h>
#include <arpa/inet.h>
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

    n->log_fp = ::fopen(LOG_FILE_PATH, "w");
    if (n->log_fp == NULL) {
        return UnexpectedF(std::format(
            "Error opening log file with path {}\n",
            LOG_FILE_PATH
        ));
    }
    n->snapshot_fp = ::fopen(SNAPSHOT_FILE_PATH, "w");
    if (n->snapshot_fp == NULL) {
        return UnexpectedF(std::format(
            "Error opening snapshot file with path {}\n",
            SNAPSHOT_FILE_PATH
        ));
    }

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
        n->loops_[i] = std::move(*loop_raw);

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
    const char* init_cluster[BASE_CLUSTER_SIZE];
    setup_peers(init_cluster);
    static_assert(static_cast<size_t>(MY_ID) >= 0 && static_cast<size_t>(MY_ID) < BASE_CLUSTER_SIZE);

    n->node_ids_.reserve(BASE_CLUSTER_SIZE - 1);
    int i = 0;
    for (; i < MY_ID; ++i) {
        n->node_ids_.push_back(i);

        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, init_cluster[i], SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
    }
    ++i;
    for (; i < BASE_CLUSTER_SIZE; ++i) {
        n->node_ids_.push_back(i);

        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, init_cluster[i], SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
    }

    n->log_.push_back(LogEntry{});
    n->next_indexes_[MY_ID] = -1;
    n->match_indexes_[MY_ID] = -1;
    n->voters_.reserve(BASE_CLUSTER_SIZE);
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

void Node::request_votes() {
    for (NodeID id : node_ids_) {
        #ifdef DEBUG
        std::cout << "sending RV to peer " << id << " on event loop " << static_cast<int>(id & (EVENT_LOOP_THREADS - 1)) << "\n";
        #endif

        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        el->outbound_inbox.PushOne(
            std::make_unique<RpcMessage>(RequestVoteReqPayload{
                .dest_id = id,
                .term = current_term_,
                .candidate_id = MY_ID,
                .last_log_idx = static_cast<uint32_t>(log_.size() - 1),
                .last_log_term = log_.back().term
            })
        );
    }
    for (auto& loop : loops_) { loop->Wake(); }
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

void Node::demote() {
    #ifdef DEBUG
    std::cout << "this node (id " << MY_ID << ") was demoted\n";
    #endif

    voters_.clear();
    voted_for_ = -1;
    NodeState old = state_;
    state_ = NodeState::Follower;

    if (old != NodeState::Leader) return;
    #ifdef DEBUG
    std::cout << "disarming peer timers\n";
    #endif
    for (auto id : node_ids_) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        el->outbound_inbox.PushOne(
            std::make_unique<RpcMessage>(
                DisarmTimer{ .dest_id = id }
            )
        );
    }
    for (auto& loop : loops_) { loop->Wake(); }
}

void Node::become_leader() {
    #ifdef DEBUG
    std::cout << "This node (id = " << MY_ID << ") won the election\n";
    #endif
    const int32_t last_log_idx = static_cast<int32_t>(log_.size() - 1);
    for (int32_t& i : next_indexes_) {
        if (i < 0) continue;
        i = last_log_idx + 1;
    }
    for (int32_t& i : match_indexes_) {
        if (i < 0) continue;
        i = 0;
    }

    voters_.clear();
    voted_for_ = -1;
    state_ = NodeState::Leader;

    #ifdef DEBUG
    std::cout << "Sending heartbeats and arming peer timers\n";
    #endif
    for (auto id : node_ids_) {
        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        // send heartbeat rpc
        const uint32_t prev_log_idx = static_cast<uint32_t>(log_.size() - 1);
        const uint32_t prev_log_term = log_[prev_log_idx].term;
        el->outbound_inbox.PushOne(
            std::make_unique<RpcMessage>(
                AppendEntriesReqPayload{
                    std::span<LogEntry>{},
                    id,
                    current_term_,
                    MY_ID,
                    prev_log_idx,
                    prev_log_term,
                    commit_index_
                }
            )
        );
        #ifdef DEBUG
        std::cout << "posted heartbeat message to peer " << id << "\n";
        #endif
        // arm this peer's heartbeat timer so we know when to send the next heartbeat.
        el->outbound_inbox.PushOne(
            // send heartbeat rpc
            std::make_unique<RpcMessage>(
                ArmTimer{ .dest_id = id }
            )
        );
        #ifdef DEBUG
        std::cout << "posted arm timer message to peer " << id << "\n";
        #endif
    }

    for (auto& loop : loops_) { loop->Wake(); }
}

void Node::add_peer_if_not_exists(NodeID node_id, FD fd, std::unique_ptr<EventLoop>& el) {
    if (std::find(node_ids_.begin(), node_ids_.end(), node_id) != node_ids_.end()) {
        return;
    }

    node_ids_.push_back(node_id);
    if (next_indexes_.size() < node_id) {
        next_indexes_.resize(node_id + 1);
        match_indexes_.resize(node_id + 1);
    }
    next_indexes_[node_id] = 1;
    match_indexes_[node_id] = 0;

    el->outbound_inbox.PushOne(
        std::make_unique<RpcMessage>(
            AddPeerMsg{ .fd = fd, .port = SERVER_PORT, .dest_id = node_id }
        )
    );
    #ifdef DEBUG
    std::cout << "added node w/ id " << node_id << " to node_ids_\n";
    #endif
}

void Node::commit_if_quorum() {
    // No peers => no quorum to compute. Guards std::max_element below from
    // dereferencing end() on an empty map.
    if (node_ids_.empty()) return;

    auto freqs = std::unordered_map<int32_t, uint32_t>(match_indexes_.size());
    for (auto match_idx : match_indexes_) {
        if (match_idx < 0) continue;
        ++freqs[match_idx];
    }

    // fast path
    auto kv_max_freq = std::max_element(freqs.begin(), freqs.end(), [](const std::pair<uint32_t, uint32_t>& a, const std::pair<uint32_t, uint32_t>& b){
       return a.second < b.second;
    });
    if (kv_max_freq == freqs.end()) return;
    if (kv_max_freq->second <= node_ids_.size() / 2) return;
    if (kv_max_freq->first > commit_index_
        && kv_max_freq->first < log_.size()
        && log_[kv_max_freq->first].term == current_term_) {
        commit_index_ = kv_max_freq->first;
        return;
    }

    // slow path
    freqs.erase(kv_max_freq);
    std::vector<std::pair<int32_t, uint32_t>> freqs_vec;
    freqs_vec.reserve(freqs.size());
    for (auto [idx, count] : freqs) {
        freqs_vec.emplace_back(idx, count);
    }
    std::sort(freqs_vec.begin(), freqs_vec.end(), [](auto& a, auto& b){ return a.second > b.second; });

    for (auto& [match_idx, count] : freqs_vec) {
        if (count <= node_ids_.size() / 2) break;
        if (match_idx > commit_index_
            && match_idx < log_.size()
            && log_[match_idx].term == current_term_) {
            commit_index_ = match_idx;
            break;
        }
    }
}
