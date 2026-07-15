// =============================================================================
// Implementation
// =============================================================================

#include "./node.hpp"
#include "./helpers.hpp"
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
std::expected<std::unique_ptr<Node>, std::string> Node::CreateNode(NodeInbox& inbox,
    void(*apply_entry_to_sm)(FILE*, const LogEntry&),
    void(*create_snapshot)(FILE*, FILE*)) {
    static_assert(EVENT_LOOP_THREADS > 0 && (EVENT_LOOP_THREADS & (EVENT_LOOP_THREADS - 1)) == 0,
        "Node: EVENT_LOOP_THREADS must be a power of 2 (MPSC inbox requires it)");
    auto n = std::unique_ptr<Node>(new Node(inbox));
    n->apply_entry = apply_entry_to_sm;
    n->create_snapshot = create_snapshot;

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

    const char* mode;

    mode = access(LOG_FILE_PATH, F_OK) == 0
        ? "r+"
        : "w+";
    n->log_fp_ = ::fopen(LOG_FILE_PATH, mode);
    if (n->log_fp_ == NULL) {
        return UnexpectedF(std::format(
            "Error opening log file with path {}\n{}\n",
            LOG_FILE_PATH, errno
        ));
    }

    #ifdef DEBUG
    ::fseek(n->log_fp_, 0, SEEK_END);
    int file_size = ::ftell(n->log_fp_);
    ::rewind(n->log_fp_);
    std::cout << "log file size = " << file_size << "\n";
    #endif

    VoidExpectedF recover_ok = n->recover();
    if (!recover_ok) {
        return UnexpectedF(std::format(
            "Failed to create node: {}\n",
            recover_ok.error()
        ));
    }

    mode = access(STATE_MACHINE_FILE_PATH, F_OK) == 0
        ? "r+"
        : "w+";
    n->sm_fp_ = ::fopen(STATE_MACHINE_FILE_PATH, mode);
    if (n->sm_fp_ == NULL) {
        return UnexpectedF(std::format(
            "Error opening state machine file with path {}\n",
            STATE_MACHINE_FILE_PATH
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
    n->last_flush_ = std::chrono::steady_clock::now();

    // init_peers is identical on every node in the cluster, so a peer's
    // index in this array IS its globally consistent NodeID. Each node lists
    // itself at index MY_ID using the placeholder "" in place of its own IP;
    // we skip that slot (the loop counter still advances so peer indices stay
    // aligned with their array positions).
    const char* init_cluster[BASE_CLUSTER_SIZE];
    setup_peers(init_cluster);
    //static_assert(static_cast<size_t>(MY_ID) < BASE_CLUSTER_SIZE, "This node's ID exceeds the cluster size");

    int i = 0;
    for (; i < MY_ID; ++i) {
        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, init_cluster[i], SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
        n->node_ids_.set(i);
    }
    ++i;
    for (; i < BASE_CLUSTER_SIZE; ++i) {
        VoidExpectedF add_peer_ok = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            ->AddPeer(i, init_cluster[i], SERVER_PORT);
        if (!add_peer_ok) {
            return UnexpectedF(
                std::format("error creating node:\n{}\n", add_peer_ok.error())
            );
        }
        n->node_ids_.set(i);
    }

    n->next_indexes_[MY_ID] = -1;
    n->match_indexes_[MY_ID] = -1;
    n->chunks_sent_[MY_ID] = -1;
    n->voters_.reserve(BASE_CLUSTER_SIZE);
    n->last_leader_contact_ = std::chrono::steady_clock::now();
    return n;
}

Node::~Node() {
    Stop();
}

void Node::Stop() {
    if (!running_) return;
    running_ = false;
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (!loops_[i]->stopped.load(std::memory_order_acquire)) loops_[i]->Stop();
        loops_[i].reset();
        if (threads_[i].joinable()) threads_[i].join();
    }
    ::fclose(log_fp_);
    ::fclose(snapshot_fp_);
    ::fclose(sm_fp_);
    if (snapshot_tmp_fp_ != nullptr) ::fclose(snapshot_tmp_fp_);
}

// ---- outbound --------------------------------------------------------

void Node::request_votes() {
    for (NodeID id = 0; id < node_ids_.total_size(); ++id) {
        if (!node_ids_[id]) continue;
        #ifdef DEBUG
        std::cout << "sending RV to peer " << id << " on event loop " << static_cast<int>(id & (EVENT_LOOP_THREADS - 1)) << "\n";
        #endif

        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        el->outbound_inbox.PushOne(
            std::make_unique<RpcMessage>(RequestVoteReqPayload{
                .dest_id = id,
                .term = current_term_,
                .candidate_id = MY_ID,
                .last_log_idx = static_cast<uint32_t>(log_.size()) + last_applied_idx_,
                .last_log_term = log_.back().term
            })
        );
        last_rv_sent_[id] = std::chrono::steady_clock::now();
    }
    for (auto& loop : loops_) { loop->Wake(); }
}

void Node::append_commands(std::vector<std::byte*>& commands) {
    //const uint32_t last_log_idx = log_.size() - 1;
    if (state_ != NodeState::Leader) {
        #ifdef DEBUG
        std::cout << "Found append request in non-leader state - skipping\n";
        #endif
        return;
    }
    #ifdef DEBUG
    std::cout << "appending commands...\n";
    std::cout << "Existing log:\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (std::byte b : e.data_) {
            std::cout << static_cast<int>(b) << ", ";
        }
        std::cout << "), " << e.term << "]\n";
    }
    #endif

    log_.reserve(log_.size() + commands.size());
    for (std::byte* command : commands) {
        log_.emplace_back(command, CMD_SIZE, current_term_);
    }
    ::fseek(log_fp_, 0, SEEK_END);
    ::fwrite(log_.data() + log_.size() - commands.size(), sizeof(LogEntry), commands.size(), log_fp_);

    #ifdef DEBUG
    std::cout << "New log:\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (std::byte b : e.data_) {
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

NodeID Node::get_leader() {
    return leader_id;
}

void Node::demote() {
    #ifdef DEBUG
    std::cout << "this node (id " << MY_ID << ") was demoted\n";
    #endif

    voters_.clear();
    voted_for_ = -1;
    write_voted_for();
    NodeState old = state_;
    state_ = NodeState::Follower;
    for (size_t i = 0; i < next_indexes_.size(); ++i) {
        if (next_indexes_[i] < 0) continue;
        next_indexes_[i] = 1;
    }
    for (size_t i = 0; i < match_indexes_.size(); ++i) {
        if (match_indexes_[i] < 0) continue;
        match_indexes_[i] = 0;
    }
    for (size_t i = 0; i < chunks_sent_.size(); ++i) {
        if (chunks_sent_[i] < 0) continue;
        chunks_sent_[i] = 0;
    }

    if (old != NodeState::Leader) return;
    #ifdef DEBUG
    std::cout << "disarming peer timers\n";
    #endif
    for (NodeID id = 0; id < node_ids_.total_size(); ++id) {
        if (!node_ids_[id]) continue;

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
    const uint32_t last_log_idx = static_cast<uint32_t>(log_.size()) + last_applied_idx_; // logical index
    for (int32_t& i : next_indexes_) {
        if (i < 0) continue;
        i = last_log_idx + 1;
    }
    for (int32_t& i : match_indexes_) { // is this needed?
        if (i < 0) continue;
        i = 0;
    }
    for (size_t& i : chunks_sent_) { // is this needed?
        if (i < 0) continue;
        i = 0;
    }

    voters_.clear();
    voted_for_ = -1;
    write_voted_for();
    state_ = NodeState::Leader;

    // placeholder entry allows this node to assert its leadership to other nodes on the next heartbeat
    log_.push_back(LogEntry(current_term_));
    #ifdef DEBUG
    std::cout << "writing placeholder log entry to file...\n";
    #endif
    ::fseek(log_fp_, 0, SEEK_END);
    ::fwrite(&log_.back(), sizeof(LogEntry), 1, log_fp_);

    #ifdef DEBUG
    std::cout << "Arming peer timers\n";
    #endif
    for (NodeID id = 0; id < node_ids_.total_size(); ++id) {
        if (!node_ids_[id]) continue;

        auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
        // send heartbeat rpc
        // const uint32_t prev_log_term = log_.back().term;
        // el->outbound_inbox.PushOne(
        //     std::make_unique<RpcMessage>(
        //         AppendEntriesReqPayload{
        //             std::span<LogEntry>{},
        //             id,
        //             current_term_,
        //             MY_ID,
        //             last_log_idx,
        //             prev_log_term,
        //             commit_index_
        //         }
        //     )
        // );
        // #ifdef DEBUG
        // std::cout << "posted heartbeat message to peer " << id << "\n";
        // #endif
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
    if (node_id < 0 || (node_id < node_ids_.total_size() && node_ids_[node_id]) || installing_snapshot_id_ >= 0) return;
    node_ids_.add(node_id);

    if (next_indexes_.size() <= node_id) {
        next_indexes_.resize(node_id + 1);
        match_indexes_.resize(node_id + 1);
        chunks_sent_.resize(node_id + 1);
        last_ae_sent_.resize(node_id + 1, std::chrono::steady_clock::time_point::max());
        last_rv_sent_.resize(node_id + 1, std::chrono::steady_clock::time_point::max());
        last_is_sent_.resize(node_id + 1, std::chrono::steady_clock::time_point::max());
    }
    next_indexes_[node_id] = 1;
    // match_indexes_, chunks_sent, last_ae_sent, last_rv_sent_, and last_is_sent_ are default initialized correctly at the corresponding index.

    el->outbound_inbox.PushOne(
        std::make_unique<RpcMessage>(
            AddPeerMsg{ .fd = fd, .port = SERVER_PORT, .dest_id = node_id }
        )
    );
    #ifdef DEBUG
    std::cout << "added node w/ id " << node_id << " to node_ids_\n";
    #endif
}

uint32_t Node::compute_new_commit_idx() {
    // No peers => no quorum to compute. Guards std::max_element below from
    // dereferencing end() on an empty map.
    // if (node_ids_.empty()) return commit_index_;
    if (node_ids_.empty()) return log_.size() + last_applied_idx_;

    auto freqs = std::unordered_map<int32_t, uint32_t>(match_indexes_.size());
    for (auto match_idx : match_indexes_) {
        if (match_idx < 0) continue;
        ++freqs[match_idx];
    }
    ++freqs[log_.size() + last_applied_idx_];

    // fast path
    auto kv_max_freq = std::max_element(freqs.begin(), freqs.end(), [](const std::pair<uint32_t, uint32_t>& a, const std::pair<uint32_t, uint32_t>& b){
       return a.second < b.second;
    });
    if (kv_max_freq == freqs.end()) return commit_index_;
    if (kv_max_freq->second <= node_ids_.size() / 2) return commit_index_;
    if (kv_max_freq->first > commit_index_
        && kv_max_freq->first - last_applied_idx_ < log_.size()
        && log_[kv_max_freq->first - last_applied_idx_].term == current_term_) {
        return kv_max_freq->first;
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
            && match_idx - last_applied_idx_ < log_.size()
            && log_[match_idx - last_applied_idx_].term == current_term_) {
            return match_idx;
        }
    }
    return commit_index_;
}

// Nodes periodically write a snapshot consisting of:
// - last included index
// - last included term
// - state machine state

// update the live state machine
// void Node::apply_entry_to_sm(const LogEntry& entry) {
//     // get the operation (add, sub, mul, div)
//     int8_t op = bytes_to_int8(entry.data_);
//     int8_t amt1 = bytes_to_int8(entry.data_ + 1);
//     int16_t amt2 = bytes_to_int16(entry.data_ + 2);
//     int32_t amt = static_cast<uint32_t>(amt1) << 16 | static_cast<uint32_t>(amt2);
//     int64_t tmp_state = bytes_to_int64(snapshot.state->data());
//     switch (op) {
//         case 0:
//             tmp_state += amt;
//             break;
//         case 1:
//             tmp_state -= amt;
//             break;
//         case 2:
//             tmp_state *= amt;
//             break;
//         case 3:
//             tmp_state /= amt;
//             break;
//     }
//     SMStateBytes tmpa = std::bit_cast<SMStateBytes>(tmp_state);
//     *snapshot.state = std::move(tmpa);
// }

VoidExpectedF Node::recover() {
    // Read existing snapshot if it exists and restore the state machine
    // if (access(SNAPSHOT_FILE_PATH, F_OK) == 0) {
    //     if (snapshot_fp_ != nullptr) ::fclose(snapshot_fp_);
    //     ::rename(SNAPSHOT_FILE_PATH, STATE_MACHINE_FILE_PATH);

    //     snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "w+");
    //     if (snapshot_fp_ == NULL) {
    //         return UnexpectedF(std::format(
    //             "Error opening snapshot file with path {}\n",
    //             SNAPSHOT_FILE_PATH
    //         ));
    //     }

    //     ::fseek(snapshot_fp_, -1 * (sizeof(uint32_t) * 2), SEEK_END);
    //     ::fread(&last_applied_idx_, sizeof(last_applied_idx_), 1, snapshot_fp_);
    //     ::fread(&last_applied_term_, sizeof(last_applied_term_), 1, snapshot_fp_);
    // }
    // else {
    //     snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "w+");
    // }

    if (access(SNAPSHOT_FILE_PATH, F_OK) == 0) {
        snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");
        if (snapshot_fp_ == NULL) {
            return UnexpectedF(std::format(
                "Error opening snapshot file with path {}\n",
                SNAPSHOT_FILE_PATH
            ));
        }
        ::fseek(snapshot_fp_, -1 * (sizeof(uint32_t) * 2), SEEK_END);
        ::fread(&last_applied_idx_, sizeof(last_applied_idx_), 1, snapshot_fp_);
        ::fread(&last_applied_term_, sizeof(last_applied_term_), 1, snapshot_fp_);

        ::fclose(snapshot_fp_);
        ::rename(SNAPSHOT_FILE_PATH, STATE_MACHINE_FILE_PATH);
    }
    snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "w+");

    ::fseek(log_fp_, 0, SEEK_END);
    long file_size = ::ftell(log_fp_);
    #ifdef DEBUG
    std::cout << "log file size = " << file_size << "\n";
    #endif
    if (file_size <= 0) return {};
    ::rewind(log_fp_);
    ::fread(&current_term_, sizeof(current_term_), 1, log_fp_);
    ::fread(&voted_for_, sizeof(voted_for_), 1, log_fp_);

    size_t num_entries = (static_cast<size_t>(file_size) - sizeof(current_term_) - sizeof(voted_for_)) / sizeof(LogEntry);
    if (num_entries == 0) return {};

    log_.resize(log_.size() + num_entries);
    ::fread(&log_.front(), sizeof(LogEntry), num_entries, log_fp_);

    // size_t start = 0;
    // if (snapshot.last_included_idx > 0 && snapshot.last_included_idx < log_.size()) {
    //     start = snapshot.last_included_idx;
    // }

    // for (size_t i = start; i < log_.size(); ++i) {
    //     apply_entry_to_sm(log_[i]);
    // }

    // snapshot.last_included_idx += static_cast<uint32_t>(log_.size() - start);
    // snapshot.last_included_term = log_.back().term;

    //::rewind(snapshot_fp); // not needed if this function only runs in the factory function
    // ::fwrite(&snapshot, sizeof(Snapshot), 1, snapshot_fp);
    // ::fflush(snapshot_fp);
    // ::fsync(fileno(snapshot_fp));

    // only needed if this function is called outside the CreateNode factory function
    // commit_index_ = snapshot.last_included_idx;
    // state_ = NodeState::Follower;
    // voters_.clear();
    // voted_for_ = -1;
    return {};
}

void Node::write_current_term() {
    #ifdef DEBUG
    std::cout << "writing current term = " << current_term_ << " to log file\n";
    #endif
    ::rewind(log_fp_);
    ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
}

void Node::write_voted_for() {
    #ifdef DEBUG
    std::cout << "writing voted for id = " << voted_for_ << " to log file\n";
    #endif
    ::fseek(log_fp_, sizeof(current_term_), SEEK_SET);
    ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
}

void Node::flush_files() {
    #ifdef DEBUG
    std::cout << "flushing files...\n";
    #endif
    if (log_fp_) { ::fflush(log_fp_); ::fsync(fileno(log_fp_)); }
    if (snapshot_fp_) { ::fflush(snapshot_fp_); ::fsync(fileno(snapshot_fp_)); }
    if (sm_fp_) { ::fflush(sm_fp_); ::fsync(fileno(sm_fp_)); }
    last_flush_ = std::chrono::steady_clock::now();
}

VoidExpectedF Node::send_append_entries(uint32_t next_idx, std::unique_ptr<EventLoop>& el, NodeID dest_id) {
    // Only send entries when the log actually has some at/after
    // next_idx. log_.size()-1 >= next_idx is restated as
    // next_idx < log_.size() to avoid uint underflow on size 0.
    const uint32_t prev_log_idx = next_idx - 1;
    const size_t prev_log_idx_offset = prev_log_idx - last_applied_idx_;
    const uint32_t prev_log_term = log_[prev_log_idx_offset].term;
    const size_t next_idx_offset = next_idx - last_applied_idx_;
    auto s = next_idx_offset < log_.size()
    ? std::span<LogEntry>(log_.begin() + next_idx_offset, log_.end())
    : std::span<LogEntry>{};

    #ifdef DEBUG
    assert(s.size() <= MAX_ENTRIES);
    std::cout << "sending " << s.size() << " entries\n";
    #endif

    auto p = AppendEntriesReqPayload{
        s.size(),
        dest_id,
        current_term_,
        MY_ID,
        prev_log_idx,
        prev_log_term,
        commit_index_
    };
    std::memcpy(p.entries, s.data(), sizeof(LogEntry) * s.size());
    send(std::move(p), el);
    last_ae_sent_[dest_id] = std::chrono::steady_clock::now();
    return {};
}

VoidExpectedF Node::send_install_snapshot(std::unique_ptr<EventLoop>& el, NodeID dest_id) {
    auto p = InstallSnapshotReqPayload{
        .cluster_raw_size = node_ids_.total_size(),
        .last_included_idx = last_applied_idx_ss_,
        .last_included_term = last_applied_term_ss_,
        .offset = chunks_sent_[dest_id] * SNAPSHOT_CHUNK_SIZE,
        .dest_id = dest_id,
        .leader_id = MY_ID,
        .done = (chunks_sent_[dest_id] + 1) * SNAPSHOT_CHUNK_SIZE >= SM_STATE_SIZE,
    };
    // adjust the cluster config for the receiving node
    node_ids_.set(MY_ID);
    node_ids_.unset(dest_id);
    std::memcpy(p.cluster, node_ids_.data(), node_ids_.total_size());
    node_ids_.unset(MY_ID);
    node_ids_.set(dest_id);

    // Write the first snapshot chunk to the payload
    ::fseek(snapshot_fp_, node_ids_.total_size() + sizeof(last_applied_idx_) + sizeof(last_applied_term_), SEEK_SET);
    if (::fread(p.partial_state, SNAPSHOT_CHUNK_SIZE, 1, snapshot_fp_) < 1) {
        return UnexpectedF(std::format(
            "failed to send InstallSnapshot request to node {} - couldn't read from snapshot file at offset 0",
            dest_id
        ));
    }
    send(std::move(p), el);
    last_is_sent_[dest_id] = std::chrono::steady_clock::now();
    return {};
}
