#pragma once
/*
Raft node.

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
#include "./helpers.hpp"
#include "../config.hpp"
#include "../rpc/event_loop/event_loop.hpp"
#include "../rpc/protocol/payloads.hpp"
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <random>
#include <array>
#include <unordered_set>
#include <vector>
#ifdef DEBUG
#include <iostream>
#endif

#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct Node {
public:
    static std::optional<std::string> CreateNode(Node*, NodeInbox*, void(*)(FILE*, const LogEntry&), void(*)(FILE*, FILE*));
    ~Node();
    Node()                       = default;
    Node(const Node&)            = delete;
    Node& operator=(const Node&) = delete;
    Node(Node&&)                 = delete;
    Node& operator=(Node&&)      = delete;

    // Signals every loop to exit, joins all worker threads.
    void Stop();

    void MainLoop();

    void append_commands(std::vector<std::byte*>&);
    // void append_commands(std::vector<int16_t>&);
    // void append_commands(std::vector<int32_t>&);
    // void append_commands(std::vector<int64_t>&);
    void append_commands(std::byte (&)[MAX_ENTRIES][CMD_SIZE], size_t num_entries);

    void forward_request(std::vector<std::byte*>&);
    void forward_request(std::byte (&)[MAX_ENTRIES][CMD_SIZE], size_t num_entries);

    int get_leader();

    // TODO: replace AoS EventLoop w/ SoA pattern
    private:
    template <typename T>
    void send(T&& payload, EventLoop& el) {
        el.outbound_inbox.PushOne(
            EventLoopMessage(std::forward<T>(payload))
        );
        el.Wake();
    }
    std::optional<std::string> send_append_entries(int32_t next_idx, EventLoop&, NodeID);
    std::optional<std::string> send_install_snapshot(EventLoop&, NodeID);
    void request_votes();

    void demote();
    void become_leader();
    void advance_to_term(uint32_t);

    void add_peer_if_not_exists(NodeID, FD, EventLoop&);
    uint32_t compute_new_commit_idx();
    void commit_entries_if_available();

    size_t snapshot_header_bytes() const;
    size_t sm_header_bytes() const;
    size_t snapshot_config_and_data_offset_bytes() const;

    /* log compaction/snapshotting/recovery */
    std::optional<std::string> recover();

    void write_current_term();
    void write_voted_for();
    void flush_files();

    void iterate_node_ids(auto&& callback);
    void print_cluster();

    std::mt19937                                                    rand_gen_                = std::mt19937(std::random_device{}());
    std::unordered_set<NodeID>                                      voters_;
    GenericBitset                                                   installing_snapshot_     = GenericBitset(BASE_CLUSTER_SIZE);
    std::vector<LogEntry>                                           log_;
    std::vector<uint64_t>                                           chunks_sent_             = std::vector<uint64_t>(BASE_CLUSTER_SIZE, 0); // after every IS RPC send, increment by 1
    std::vector<int32_t>                                            next_indexes_            = std::vector<int32_t>(BASE_CLUSTER_SIZE, 1);         // leader-only, one per peer
    std::vector<int32_t>                                            match_indexes_           = std::vector<int32_t>(BASE_CLUSTER_SIZE, 0);         // leader-only, one per peer
    NodeBitset                                                      node_ids_                = NodeBitset(BASE_CLUSTER_SIZE);

    std::array<EventLoop, EVENT_LOOP_THREADS>                       loops_{};
    std::array<std::thread, EVENT_LOOP_THREADS>                     threads_;

    std::chrono::steady_clock::time_point                           last_leader_contact_;
    std::chrono::steady_clock::time_point                           last_flush_;
    std::chrono::milliseconds                                       election_timeout_;     // Election timeout, randomized at construction.
    std::uniform_int_distribution<>                                 distrib_                 = std::uniform_int_distribution<>(MIN_ELECTION_TIMEOUT_MS, MAX_ELECTION_TIMEOUT_MS);
    NodeInbox*                                                      inbox_;
    FILE*                                                           log_fp_                  = nullptr;
    FILE*                                                           snapshot_fp_             = nullptr;
    FILE*                                                           snapshot_tmp_fp_         = nullptr;
    FILE*                                                           sm_fp_                   = nullptr;
    void(*apply_entry)(FILE*, const LogEntry&);
    void(*create_snapshot)(FILE*, FILE*);
    int                                                             leader_id_               = -1;
    int                                                             voted_for_               = -1;
    uint32_t                                                        base_logical_idx_        = 1; // logical indexes are 1-based
    uint32_t                                                        base_term_               = 0;
    uint32_t                                                        last_applied_idx_        = 0;
    uint32_t                                                        last_applied_term_       = 0;
    uint32_t                                                        current_term_            = 0;
    uint32_t                                                        commit_index_            = 0;     // index of highest log entry known to be committed
    enum class                                                      NodeState { Follower, Candidate, Leader };
    NodeState                                                       state_                   = NodeState::Follower;
    bool                                                            running_                 = false;
};


// Factory function
// Node requires stable addresses (i.e. not movable)
inline std::optional<std::string> Node::CreateNode(Node* n, NodeInbox* inbox,
    void(*apply_entry_to_sm)(FILE*, const LogEntry&),
    void(*create_snapshot)(FILE*, FILE*)) {
    static_assert(EVENT_LOOP_THREADS > 0 && (EVENT_LOOP_THREADS & (EVENT_LOOP_THREADS - 1)) == 0,
        "Node: EVENT_LOOP_THREADS must be a power of 2 (MPSC inbox requires it)");
    static_assert(SNAPSHOT_CHUNK_SIZE >= MAX_CLUSTER_HEADER_SIZE,
        "Node: SNAPSHOT_CHUNK_SIZE must be at least as large as MAX_CLUSTER_HEADER_SIZE (determined by MAX_NODES)");

    n->inbox_ = inbox;
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

    n->election_timeout_ = std::chrono::milliseconds(n->distrib_(n->rand_gen_));
    #ifdef DEBUG
    std::cout << "election timeout set to " << n->election_timeout_ << "\n";
    #endif

    const char* mode;

    mode = access(LOG_FILE_PATH, F_OK) == 0
        ? "r+"
        : "w+";
    n->log_fp_ = ::fopen(LOG_FILE_PATH, mode);
    if (n->log_fp_ == NULL) {
        return (std::format(
            "Error opening log file with path {}\n{}\n",
            LOG_FILE_PATH, errno
        ));
    }

    #ifdef DEBUG
    struct stat st;
    if (stat(LOG_FILE_PATH, &st) == 0) {
        std::cout << "log file size = " << st.st_size << "\n";
    }
    #endif

    std::optional<std::string> recover_err = n->recover();
    if (recover_err) {
        return (std::format(
            "Failed to create node: {}\n",
            recover_err.value()
        ));
    }

    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        //n.loops_[i] = std::move(*loop_raw);
        std::optional<std::string> create_el_err = EventLoop::CreateEventLoop(
            &n->loops_[i], inbox, i, HEARTBEAT_INTERVAL_MS, RPC_TIMEOUT_MS
        );
        if (create_el_err) {
            return (
                std::format("error creating event loop {}:\n{}\n", i, create_el_err.value())
            );
        }

        n->threads_[i] = std::thread([n, i] {
            std::optional<std::string> loop_err = n->loops_[i].Run();
            // #ifdef DEBUG
            // std::cout << "event loop " << i << " crashed:\n" << loop_err.value() << "\n";
            // #endif
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

    for (int i = 0; i < BASE_CLUSTER_SIZE; ++i) {
        n->node_ids_.set_cluster_node(i);
        if (i == MY_ID) continue;
        std::optional<std::string> add_peer_err = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
            .AddPeer(i, init_cluster[i], SERVER_PORT);
        if (add_peer_err) {
            return (
                std::format("error creating node:\n{}\n", add_peer_err.value())
            );
        }
        n->node_ids_.set_online_node(i);
    }

    n->next_indexes_[MY_ID] = -1;
    n->match_indexes_[MY_ID] = -1;
    n->chunks_sent_[MY_ID] = -1;
    n->voters_.reserve(BASE_CLUSTER_SIZE);
    n->last_leader_contact_ = std::chrono::steady_clock::now();
    return {};
}

inline Node::~Node() {
    running_ = false;
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (!loops_[i].stopped.load(std::memory_order_acquire)) loops_[i].Stop();
        if (threads_[i].joinable()) threads_[i].join();
    }

    ::fclose(log_fp_);
    if (snapshot_fp_ != nullptr) ::fclose(snapshot_fp_);
    ::fclose(sm_fp_);
    if (snapshot_tmp_fp_ != nullptr) ::fclose(snapshot_tmp_fp_);
}

inline void Node::Stop() {
    inbox_->Push(0, StopNodeMsg{});
}

// ---- outbound --------------------------------------------------------

inline void Node::request_votes() {
    iterate_node_ids([&](size_t id){
        #ifdef DEBUG
        std::cout << "sending RV to peer " << id << " on event loop " << static_cast<int>(id & (EVENT_LOOP_THREADS - 1)) << "\n";
        #endif
        auto& el = this->loops_[id & (EVENT_LOOP_THREADS - 1)];
        el.outbound_inbox.PushOne(
            EventLoopMessage(RequestVoteReqPayload{
                .dest_id = static_cast<NodeID>(id),
                .term = current_term_,
                .candidate_id = MY_ID,
                .last_log_idx = static_cast<uint32_t>(log_.size() - 1) + base_logical_idx_,
                .last_log_term = log_.empty() ? base_term_ : log_.back().term
            })
        );
    });

    for (auto& loop : loops_) { loop.Wake(); }
}

inline void Node::append_commands(std::vector<std::byte*>& commands) {
    //const uint32_t last_log_idx = log_.size() - 1;
    #ifdef DEBUG
    std::cout << "Found append request; state = " << static_cast<int>(state_) << "; leader_id = " << leader_id_ << "\n";
    #endif
    if (state_ != NodeState::Leader) {
        if (leader_id_ != -1) {
            #ifdef DEBUG
            std::cout << "Not in non-leader state - forwarding...\n";
            #endif
            forward_request(commands);
        }
        return;
    }
    #ifdef DEBUG
    std::cout << "appending commands...\n";
    std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
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

    commit_entries_if_available();

    #ifdef DEBUG
    std::cout << "New log (size = " << log_.size() << "):\n";
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

inline void Node::append_commands(std::byte (&commands)[MAX_ENTRIES][CMD_SIZE], size_t num_entries) {
    //const uint32_t last_log_idx = log_.size() - 1;
    #ifdef DEBUG
    std::cout << "Found append request; state = " << static_cast<int>(state_) << "; leader_id = " << leader_id_ << "\n";
    #endif
    if (state_ != NodeState::Leader) {
        if (leader_id_ != -1) {
            #ifdef DEBUG
            std::cout << "Not in non-leader state - forwarding...\n";
            #endif
            forward_request(commands, num_entries);
        }
        return;
    }
    #ifdef DEBUG
    std::cout << "appending commands...\n";
    std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
    std::cout << "Existing log:\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (std::byte b : e.data_) {
            std::cout << static_cast<int>(b) << ", ";
        }
        std::cout << "), " << e.term << "]\n";
    }
    #endif

    log_.reserve(log_.size() + num_entries);
    for (int i = 0; i < num_entries; ++i) {
        log_.emplace_back(commands[i], CMD_SIZE, current_term_);
    }
    ::fseek(log_fp_, 0, SEEK_END);
    ::fwrite(log_.data() + log_.size() - num_entries, sizeof(LogEntry), num_entries, log_fp_);

    commit_entries_if_available();

    #ifdef DEBUG
    std::cout << "New log (size = " << log_.size() << "):\n";
    for (const LogEntry& e : log_) {
        std::cout << "[(";
        for (std::byte b : e.data_) {
            std::cout << static_cast<int>(b) << ", ";
        }
        std::cout << "), " << e.term << "]\n";
    }
    #endif
}

inline void Node::forward_request(std::vector<std::byte*>& commands) {
    auto& el = loops_[leader_id_ & (EVENT_LOOP_THREADS - 1)];
    size_t sent{0};
    while (sent < commands.size()) {
        size_t num_entries = std::min(commands.size() - sent, MAX_ENTRIES);
        ForwardLeaderMsg msg{
            .entries_len = num_entries,
            .sender_id = MY_ID,
            .dest_id = static_cast<NodeID>(leader_id_),
            .term = current_term_
        };
        for (size_t i = 0; i < num_entries; ++i) {
            std::memcpy(msg.entries[i], commands[sent + i], CMD_SIZE);
        }
        el.outbound_inbox.PushOne(EventLoopMessage(std::move(msg)));
        sent += num_entries;
    }
    el.Wake();
    return;
}

inline void Node::forward_request(std::byte (&commands)[MAX_ENTRIES][CMD_SIZE], size_t num_entries) {
    auto& el = loops_[leader_id_ & (EVENT_LOOP_THREADS - 1)];
    ForwardLeaderMsg msg{
        .entries_len = num_entries,
        .sender_id = MY_ID,
        .dest_id = static_cast<NodeID>(leader_id_),
        .term = current_term_
    };
    std::memcpy(&msg.entries, &commands, CMD_SIZE * num_entries);
    el.outbound_inbox.PushOne(msg);
}

inline int Node::get_leader() {
    return leader_id_;
}

inline void Node::advance_to_term(uint32_t term) {
    #ifdef DEBUG
    std::cout << "this node (id " << MY_ID << ") advanced to term " << term << "\n";
    #endif

    current_term_ = term;
    voted_for_ = -1;
    voters_.clear();
    write_current_term();
    write_voted_for();
    demote();
}

inline void Node::demote() {
    #ifdef DEBUG
    std::cout << "this node (id " << MY_ID << ") was demoted\n";
    #endif

    NodeState old = state_;
    if (old == NodeState::Follower) return;
    state_ = NodeState::Follower;
    for (size_t i = 0; i < next_indexes_.size(); ++i) {
        if (next_indexes_[i] < 0) continue;
        next_indexes_[i] = static_cast<uint32_t>(log_.size()) + base_logical_idx_; // last log idx + 1
        match_indexes_[i] = 0;
        chunks_sent_[i] = 0;
    }
    installing_snapshot_.reset();

    if (old != NodeState::Leader) return;
    #ifdef DEBUG
    std::cout << "disarming peer timers\n";
    #endif

    iterate_node_ids([&](size_t id){
        auto& el = this->loops_[id & (EVENT_LOOP_THREADS - 1)];
        el.outbound_inbox.PushOne(
            EventLoopMessage(
                DisarmTimer{ .dest_id = static_cast<NodeID>(id) }
            )
        );
    });

    for (auto& loop : loops_) { loop.Wake(); }
}

inline void Node::become_leader() {
    #ifdef DEBUG
    std::cout << "This node (id = " << MY_ID << ") won the election\n";

    std::cout << "log.size() = " << log_.size() << "\n";
    std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
    #endif

    leader_id_ = MY_ID;
    state_ = NodeState::Leader;
    // voters_.clear();
    // voted_for_ = -1;
    // write_voted_for();

    // placeholder entry allows this node to assert its leadership to other nodes on the next heartbeat
    log_.push_back(LogEntry(current_term_));

    const uint32_t last_log_idx = static_cast<uint32_t>(log_.size() - 1) + base_logical_idx_; // logical index
    for (int i = 0; i < next_indexes_.size(); ++i) {
        if (next_indexes_[i] < 0) continue;
        next_indexes_[i] = last_log_idx + 1;
        match_indexes_[i] = 0;
        chunks_sent_[i] = 0;
    }
    #ifdef DEBUG
    std::cout << "writing placeholder log entry to file...\n";
    #endif
    ::fseek(log_fp_, 0, SEEK_END);
    ::fwrite(&log_.back(), sizeof(LogEntry), 1, log_fp_);

    // ++last_applied_idx_;
    // last_applied_term_ = current_term_;

    #ifdef DEBUG
    std::cout << "Arming peer timers\n";
    #endif

    iterate_node_ids([&](size_t id){
        auto& el = this->loops_[id & (EVENT_LOOP_THREADS - 1)];
        // arm this peer's heartbeat timer so we know when to send the next heartbeat.
        el.outbound_inbox.PushOne(
            EventLoopMessage(
                ArmTimer{ .dest_id = static_cast<NodeID>(id) }
            )
        );

        #ifdef DEBUG
        std::cout << "posted arm timer message to peer " << id << "\n";
        #endif
    });

    for (auto& loop : loops_) { loop.Wake(); }
}

inline void Node::add_peer_if_not_exists(NodeID node_id, FD fd, EventLoop& el) {
    if (node_ids_.is_available(node_id)) return;

    // The peer is unknown or was dropped earlier. (Re)establish it as a live peer.
    if (next_indexes_.size() <= node_id) {
        next_indexes_.resize(node_id + 1);
        match_indexes_.resize(node_id + 1);
        chunks_sent_.resize(node_id + 1);
    }
    next_indexes_[node_id] = static_cast<uint32_t>(log_.size()) + base_logical_idx_;
    chunks_sent_[node_id] = 0;

    if (!node_ids_.is_in_cluster(node_id)) {
        match_indexes_[node_id] = 0;
        node_ids_.set_cluster_node(node_id);
    }
    node_ids_.set_online_node(node_id);

    el.outbound_inbox.PushOne(
        EventLoopMessage(
            AddPeerMsg{ .fd = fd, .port = SERVER_PORT, .dest_id = node_id }
        )
    );

    if (state_ == NodeState::Leader) {
        el.outbound_inbox.PushOne(
            EventLoopMessage(
                ArmTimer{ .dest_id = node_id }
            )
        );
    }

    el.Wake();
    #ifdef DEBUG
    std::cout << "added node w/ id " << node_id << " to node_ids_\n";
    #endif
}

inline uint32_t Node::compute_new_commit_idx() {
    if (log_.empty()) return commit_index_;

    const uint32_t last_log_idx = (log_.size() - 1) + base_logical_idx_;

    const size_t majority = (node_ids_.num_in_cluster - 1) / 2;

    // every member's last-known match index; self is always last_log_idx
    std::vector<int> matches{};
    matches.reserve(match_indexes_.size() + 1);
    for (int m : match_indexes_) {
        if (m < 0) continue;
        matches.push_back(m);
    }
    matches.push_back(last_log_idx);
    if (majority >= matches.size()) return commit_index_;
    std::nth_element(matches.begin(), matches.begin() + majority, matches.end());

    const uint32_t N = matches[majority];
    if (N <= commit_index_) return commit_index_;

    const size_t offset = N - base_logical_idx_;
    if (offset >= log_.size() || log_[offset].term != current_term_) return commit_index_;
    return N;
}

inline void Node::commit_entries_if_available() {
    // commit newly appended entries if possible
    // TODO: notify client that entries were committed
    uint32_t new_commit_idx = compute_new_commit_idx();
    #ifdef DEBUG
    std::cout << "current commit idx = " << commit_index_ << "\n";
    std::cout << "match indexes: ";
    for (auto n : match_indexes_) {
        std::cout << n << ", ";
    }
    std::cout << "\n";

    if (new_commit_idx == commit_index_) {
        std::cout << "nothing to commit\n";
    }
    else {
        std::cout << "new commit idx = " << new_commit_idx << "\n";
    }
    #endif
    if (new_commit_idx == commit_index_) return;
    commit_index_ = new_commit_idx;

    #ifdef DEBUG
    std::cout << "applying entries from last applied index = " << last_applied_idx_ + 1 << " to commit index = " << commit_index_ << "\n";
    print_cluster();
    #endif
    if (last_applied_idx_ != commit_index_) {
        for (size_t i = last_applied_idx_ + 1; i <= commit_index_; ++i) {
            #ifdef DEBUG
            std::cout << "applying entry at logical index " << i << "\n";
            #endif
            apply_entry(sm_fp_, log_[i - base_logical_idx_]);
        }
        // last_applied_term_ = log_.empty() ? last_applied_term_ : log_[commit_index_ - (last_applied_idx_ + 1)].term;
        last_applied_idx_ = commit_index_;
        last_applied_term_ = log_.empty() ? base_term_ : log_[commit_index_ - base_logical_idx_].term;
        #ifdef DEBUG
        std::cout << "last_applied_idx_ set to " << last_applied_idx_ << "\n";
        std::cout << "last_applied_term_ set to " << last_applied_term_ << "\n";
        #endif
    }
}

/*
    If the snapshot file exists and contains data, open it
    and copy its state data to a separate temp file
    Atomically rename the temp file to become the new state file
    If the log file exists and has data,
    iterate through each entry and apply each one to the state machine
    Create a new snapshot and store it on disk
    Clear the log

 */
inline std::optional<std::string> Node::recover() {
    // Read existing snapshot if it exists and restore the state machine
    struct stat snapshot_stat;
    bool snapshot_restored = false;

    if (stat(SNAPSHOT_FILE_PATH, &snapshot_stat) == 0
        && snapshot_stat.st_size >= static_cast<off_t>(
            sizeof(size_t)
            + sizeof(last_applied_idx_) + sizeof(last_applied_term_))) {
        snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");
        if (snapshot_fp_ == nullptr) {
            return (std::format(
                "Error opening snapshot file with path {}\n",
                SNAPSHOT_FILE_PATH
            ));
        }

        ::fread(&last_applied_idx_, sizeof(last_applied_idx_), 1, snapshot_fp_);
        ::fread(&last_applied_term_, sizeof(last_applied_term_), 1, snapshot_fp_);
        size_t cluster_size_bytes;
        ::fread(&cluster_size_bytes, sizeof(cluster_size_bytes), 1, snapshot_fp_);
        node_ids_.reset_cluster(snapshot_fp_, cluster_size_bytes);

        FILE* sm_tmp_fp = ::fopen(STATE_MACHINE_TMP_FILE_PATH, "w+");
        if (sm_tmp_fp == nullptr) {
            return (std::format(
                "Error opening state machine file with path {}\n",
                STATE_MACHINE_FILE_PATH
            ));
        }

        __off64_t zero{0};
        __off64_t snapshot_offset = static_cast<__off64_t>(snapshot_config_and_data_offset_bytes());
        ssize_t n = copy_file_range(fileno(snapshot_fp_), &snapshot_offset,
                                    fileno(sm_tmp_fp), &zero,
                                    snapshot_stat.st_size - snapshot_config_and_data_offset_bytes(), 0);

        ::fflush(sm_tmp_fp);
        ::fsync(fileno(sm_tmp_fp));
        ::fclose(sm_tmp_fp);
        ::rename(STATE_MACHINE_TMP_FILE_PATH, STATE_MACHINE_FILE_PATH);

        ::fclose(snapshot_fp_);
        snapshot_fp_ = nullptr;
        snapshot_restored = true;
    }

    // Open the state machine file
    const char* sm_mode = access(STATE_MACHINE_FILE_PATH, F_OK) == 0
        ? "r+"
        : "w+";
    sm_fp_ = ::fopen(STATE_MACHINE_FILE_PATH, sm_mode);
    if (sm_fp_ == nullptr) {
        return (std::format(
            "Error opening state machine file with path {}\n",
            STATE_MACHINE_FILE_PATH
        ));
    }

    // Read the log file header and entries
    struct stat log_stat;
    bool read_log_entries = stat(LOG_FILE_PATH, &log_stat) == 0 && log_stat.st_size != 0;

    if (read_log_entries) {
        ::fread(&current_term_, sizeof(current_term_), 1, log_fp_);
        ::fread(&voted_for_, sizeof(voted_for_), 1, log_fp_);

        size_t num_entries = (static_cast<size_t>(log_stat.st_size) - sizeof(current_term_) - sizeof(voted_for_)) / sizeof(LogEntry);
        if (num_entries > 0) {
            log_.resize(log_.size() + num_entries);
            ::fread(&log_.front(), sizeof(LogEntry), num_entries, log_fp_);
        }

    }

    // Apply all log entries to the state machine. After a previous log
    // compaction the log file starts at logical index last_applied_idx_ + 1,
    // so vector index 0 corresponds to logical index last_applied_idx_ + 1
    // (when a snapshot was restored), or to 0 (when no snapshot exists).
    for (size_t v = 0; v < log_.size(); ++v) {
        apply_entry(sm_fp_, log_[v]);
        ++last_applied_idx_;
        last_applied_term_ = log_[v].term;
    }

    // Clear the log file, keeping only term and voted_for
    ::freopen(LOG_FILE_PATH, "w+", log_fp_);
    ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
    ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
    log_.clear();

    // Create a new snapshot reflecting the fully replayed state
    FILE* tmp_snap = ::fopen(SNAPSHOT_TMP_FILE_PATH, "w+");
    ::fwrite(&last_applied_idx_, sizeof(last_applied_idx_), 1, tmp_snap);
    ::fwrite(&last_applied_term_, sizeof(last_applied_term_), 1, tmp_snap);
    size_t cluster_size = node_ids_.bytes();
    ::fwrite(&cluster_size, sizeof(cluster_size), 1, tmp_snap);
    ::fwrite(node_ids_.cluster.data(), cluster_size, 1, tmp_snap);
    create_snapshot(tmp_snap, sm_fp_);
    ::fflush(tmp_snap);
    ::fsync(fileno(tmp_snap));
    ::fclose(tmp_snap);
    ::rename(SNAPSHOT_TMP_FILE_PATH, SNAPSHOT_FILE_PATH);

    // if (!snapshot_restored && !read_log_entries) {
    //     // Nothing to compact; open snapshot file for runtime and return
    //     snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "w+");
    //     if (snapshot_fp_ == nullptr) {
    //         return UnexpectedF(std::format(
    //             "Error opening snapshot file with path {}\n",
    //             SNAPSHOT_FILE_PATH
    //         ));
    //     }
    //     return {};
    // }

    // Open snapshot file for runtime use
    // if (snapshot_fp_ != nullptr) ::fclose(snapshot_fp_);
    snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");
    if (snapshot_fp_ == nullptr) {
        return (std::format(
            "Error opening snapshot file with path {}\n",
            SNAPSHOT_FILE_PATH
        ));
    }

    // Advance commit_index to reflect the fully compacted state
    commit_index_ = last_applied_idx_;

    base_logical_idx_ = last_applied_idx_ + 1;
    base_term_ = last_applied_term_;

    // Seek state machine file to end for future appends
    //::fseek(sm_fp_, 0, SEEK_END);
    return {};
}

inline void Node::write_current_term() {
    #ifdef DEBUG
    std::cout << "writing current term = " << current_term_ << " to log file\n";
    #endif
    ::rewind(log_fp_);
    ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
}

inline void Node::write_voted_for() {
    #ifdef DEBUG
    std::cout << "writing voted for id = " << voted_for_ << " to log file\n";
    #endif
    ::fseek(log_fp_, sizeof(current_term_), SEEK_SET);
    ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
}

inline void Node::flush_files() {
    #ifdef DEBUG
    std::cout << "flushing files...\n";
    #endif
    if (log_fp_) { ::fflush(log_fp_); ::fsync(fileno(log_fp_)); }
    if (snapshot_fp_) { ::fflush(snapshot_fp_); ::fsync(fileno(snapshot_fp_)); }
    if (sm_fp_) { ::fflush(sm_fp_); ::fsync(fileno(sm_fp_)); }
    last_flush_ = std::chrono::steady_clock::now();
}

inline std::optional<std::string> Node::send_append_entries(int32_t next_idx, EventLoop& el, NodeID dest_id) {
    #ifdef DEBUG
    std::cout << "checking for entries to send to node " << dest_id << "\n";
    std::cout << "next index = " << next_idx << "\n";
    std::cout << "base logical idx = " << base_logical_idx_ << "\n";
    std::cout << "log_.size() == " << log_.size() << "\n";
    #endif
    // Only send entries when the log actually has some at/after
    // next_idx. log_.size()-1 >= next_idx is restated as
    // next_idx < log_.size() to avoid uint underflow on size 0.
    // A follower whose next_idx is below the compaction boundary is behind
    // the snapshot; prev_log_idx - base_logical_idx_ would underflow, so
    // callers must route such followers through InstallSnapshot instead.
    if (next_idx < base_logical_idx_) {
        return std::format(
            "send_append_entries called with next_index {} below snapshot boundary {} for node id {}; InstallSnapshot required",
            next_idx, base_logical_idx_, dest_id
        );
    }
    const uint32_t prev_log_idx = next_idx - 1;
    #ifdef DEBUG
    std::cout << "prev_log_idx = " << prev_log_idx << "\n";
    #endif
    const uint32_t prev_log_term = prev_log_idx == base_logical_idx_ - 1 ? base_term_ : log_[prev_log_idx - base_logical_idx_].term;
    #ifdef DEBUG
    std::cout << "prev_log_term = " << prev_log_term << "\n";
    #endif
    const size_t next_idx_offset = next_idx - base_logical_idx_;
    #ifdef DEBUG
    std::cout << "next_idx_offset = " << next_idx_offset << "\n";
    #endif

    auto s = next_idx_offset < log_.size()
    ? std::span<LogEntry>(
        log_.begin() + next_idx_offset, log_.end())
        .first(
            std::min(MAX_ENTRIES, log_.size() - next_idx_offset)
        )
    : std::span<LogEntry>{};

    #ifdef DEBUG
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
    return {};
}

inline size_t Node::snapshot_header_bytes() const {
    return sizeof(last_applied_idx_) + sizeof(last_applied_term_)
        + sizeof(node_ids_.bytes()) + node_ids_.bytes();
}

inline size_t Node::sm_header_bytes() const {
    return sizeof(node_ids_.bytes()) + node_ids_.bytes();
}

inline size_t Node::snapshot_config_and_data_offset_bytes() const {
    return sizeof(last_applied_idx_) + sizeof(last_applied_term_);
}

inline std::optional<std::string> Node::send_install_snapshot(EventLoop& el, NodeID dest_id) {
    #ifdef DEBUG
    std::cout << "Sending InstallSnapshot RPC:\n";
    std::cout << "term = " << current_term_ << "\n";
    std::cout << "chunk = " << chunks_sent_[dest_id] << "\n";
    std::cout << "dest id = " << dest_id << "\n";
    std::cout << "leader_id = " << MY_ID << "\n";
    #endif

    struct stat snapshot_stat;
    if (stat(SNAPSHOT_FILE_PATH, &snapshot_stat) != 0) {
        return "Failed to send InstallSnapshot RPC: couldn't get snapshot file size\n";
    }

    const uint8_t done = (chunks_sent_[dest_id] + 1) * SNAPSHOT_CHUNK_SIZE > snapshot_stat.st_size;
    #ifdef DEBUG
    std::cout << "done = " << static_cast<int>(done) << "\n";
    #endif

    auto p = InstallSnapshotReqPayload {
        .data_len = done ? snapshot_stat.st_size - (chunks_sent_[dest_id] * SNAPSHOT_CHUNK_SIZE) : SNAPSHOT_CHUNK_SIZE,
        .last_included_idx = base_logical_idx_ - 1,
        .last_included_term = base_term_,
        .offset = chunks_sent_[dest_id] * SNAPSHOT_CHUNK_SIZE,
        .dest_id = dest_id,
        .term = current_term_,
        .leader_id = MY_ID,
        .done = done
    };

    ::fseek(snapshot_fp_, p.offset, SEEK_SET);
    ::fread(p.partial_state, p.data_len, 1, snapshot_fp_); // TODO: error handling

    send(std::move(p), el);
    return {};
}

inline void Node::iterate_node_ids(auto&& callback) {
    uint64_t bitset;
    for (size_t k = 0; k < node_ids_.online.size(); ++k) {
        bitset = node_ids_.online[k];
        while (bitset != 0) {
          uint64_t t = bitset & -bitset;
          int r = __builtin_ctzl(bitset);
          callback(k * 64 + r);
          bitset ^= t;
        }
    }
}

inline void Node::print_cluster() {
    std:: cout << "current cluster: " << MY_ID << ", ";
    iterate_node_ids([](size_t id){
        std::cout << id << ", ";
    });

    std::cout << "\n";
}

#include "./main_loop.hpp"
