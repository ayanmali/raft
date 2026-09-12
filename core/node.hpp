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
#include "../rpc/event_loop/main_loop.hpp"
#include "../rpc/protocol/payloads.hpp"
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
    static std::optional<std::string> CreateNode(Node*, ELNodeInbox*, ClientNodeInbox*, void(*)(FILE*, const LogEntry&));
    ~Node();
    Node()                       = default;
    Node(const Node&)            = delete;
    Node& operator=(const Node&) = delete;
    Node(Node&&)                 = delete;
    Node& operator=(Node&&)      = delete;

    // Signals every loop to exit, joins all worker threads.
    void Stop();

    std::optional<std::string> MainLoop();

    void append_commands(std::vector<std::byte*>&);
    void append_commands(std::byte (&)[MAX_ENTRIES][CMD_SIZE], size_t num_entries);
    void append_commands(std::vector<LogEntry>&&);

    void read_state(FILE* out);

    int get_leader();

    // TODO: replace AoS EventLoop w/ SoA pattern
    private:
    template <typename T>
    void send(T&& payload, EventLoop<SOCKET_TYPE>& el) {
        el.outbound_inbox.PushOne(
            EventLoopMessage(std::forward<T>(payload))
        );
        el.Wake();
    }
    std::optional<std::string> send_append_entries(int32_t next_idx, EventLoop<SOCKET_TYPE>&, NodeID);
    std::optional<std::string> send_install_snapshot(EventLoop<SOCKET_TYPE>&, NodeID);
    void request_votes();

    void append_commands_local(std::vector<LogEntry>&&);
    void forward_request(const std::vector<LogEntry>&);

    void advance_to_term(uint32_t);
    std::optional<const char*> demote();
    void become_leader();

    std::optional<const char*> register_fd(FD fd, uint32_t events);
    std::optional<const char*> set_timer_periodic(FD fd, uint64_t secs, uint64_t nsecs);
    std::optional<const char*> set_timer(FD fd, uint64_t secs, uint64_t nsecs);
    std::optional<const char*> reset_timer(FD fd, uint64_t secs, uint64_t nsecs);
    void randomize_election_timeout();

    std::optional<std::string> OnWake(bool& leader_contact);
    std::optional<std::string> OnElectionTimeout();
    std::optional<std::string> OnHeartbeat();
    std::optional<std::string> OnFlush();

    void add_peer_if_not_exists(NodeID, IPAddrPort, EventLoop<SOCKET_TYPE>&);
    uint32_t compute_new_commit_idx();
    void commit_entries_if_available();

    std::optional<std::string> compact();

    std::optional<std::string> recover();

    std::optional<std::string> reconstruct_state(FILE* out, uint32_t up_to_idx);

    /* Helpers */
    void wake_self();
    size_t snapshot_header_bytes() const;
    size_t sm_header_bytes() const;
    size_t snapshot_config_and_data_offset_bytes() const;

    void write_current_term();
    void write_voted_for();

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

    std::array<EventLoop<SOCKET_TYPE>, EVENT_LOOP_THREADS>          loops_{};
    std::array<std::jthread, EVENT_LOOP_THREADS>                    threads_;

    std::uniform_int_distribution<>                                 distrib_                 = std::uniform_int_distribution<>(MIN_ELECTION_TIMEOUT_NS, MAX_ELECTION_TIMEOUT_NS);
    ELNodeInbox*                                                    el_inbox_;
    ClientNodeInbox*                                                client_inbox_;
    FILE*                                                           log_fp_                  = nullptr;
    FILE*                                                           snapshot_fp_             = nullptr;
    FILE*                                                           snapshot_tmp_fp_         = nullptr;
    void(*apply_entry)(FILE*, const LogEntry&);
    void(*create_snapshot)(FILE*, FILE*);

    uint64_t                                                        election_timeout_secs_;
    uint64_t                                                        election_timeout_nsecs_;
    uint64_t                                                        heartbeat_period_secs_;
    uint64_t                                                        heartbeat_period_nsecs_;
    uint64_t                                                        flush_period_secs_;
    uint64_t                                                        flush_period_nsecs_;

    NodeID                                                          leader_id_               = -1;
    NodeID                                                          voted_for_               = -1;

    FD                                                              epoll_fd_                = -1;
    FD                                                              event_fd_                = -1;
    FD                                                              election_timeout_fd_     = -1;
    FD                                                              heartbeat_fd_            = -1;
    FD                                                              flush_fd_                = -1;

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
inline std::optional<std::string> Node::CreateNode(Node* n, ELNodeInbox* el_inbox, ClientNodeInbox* client_inbox,
    void(*apply_entry_to_sm)(FILE*, const LogEntry&)) {
        static_assert(EVENT_LOOP_THREADS > 0 && (EVENT_LOOP_THREADS & (EVENT_LOOP_THREADS - 1)) == 0,
            "Node: EVENT_LOOP_THREADS must be a power of 2 (MPSC inbox requires it)");
        static_assert(SNAPSHOT_CHUNK_SIZE >= MAX_CLUSTER_HEADER_SIZE,
            "Node: SNAPSHOT_CHUNK_SIZE must be at least as large as MAX_CLUSTER_HEADER_SIZE (determined by MAX_NODES)");

        n->el_inbox_ = el_inbox;
        n->client_inbox_ = client_inbox;
        n->apply_entry = apply_entry_to_sm;
        n->running_ = true;

        n->next_indexes_[MY_ID] = -1;
        n->match_indexes_[MY_ID] = -1;
        n->chunks_sent_[MY_ID] = -1;
        n->voters_.reserve(BASE_CLUSTER_SIZE);

        n->randomize_election_timeout();
        n->heartbeat_period_secs_ = HEARTBEAT_INTERVAL_NS / NS_PER_SEC;
        n->heartbeat_period_nsecs_ = HEARTBEAT_INTERVAL_NS % NS_PER_SEC;
        n->flush_period_secs_ = FLUSH_INTERVAL_NS / NS_PER_SEC;
        n->flush_period_nsecs_ = FLUSH_INTERVAL_NS % NS_PER_SEC;

        n->epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        n->event_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        n->election_timeout_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        n->heartbeat_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        n->flush_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

        if (n->epoll_fd_ < 0 || n->event_fd_ < 0 || n->election_timeout_fd_ < 0 || n->heartbeat_fd_ < 0 || n->flush_fd_ < 0) {
            return "failed to create Node - failed to create FDs\n";
        }

        uint8_t err{0};
        int shift = -1;
        auto res = n->register_fd(n->event_fd_, EPOLLIN | EPOLLET);
        err |= bool(res) << ++shift;
        res = n->register_fd(n->election_timeout_fd_, EPOLLIN | EPOLLET);
        err |= (bool(res) << ++shift);
        res = n->register_fd(n->heartbeat_fd_, EPOLLIN | EPOLLET);
        err |= (bool(res) << ++shift);
        res = n->register_fd(n->flush_fd_, EPOLLIN | EPOLLET);
        err |= (bool(res) << ++shift);

        if (err) {
            return "Failed to create Node - failed to register FDs\n";
        }

        n->set_timer(n->election_timeout_fd_, n->election_timeout_secs_, n->election_timeout_nsecs_);
        n->set_timer_periodic(n->flush_fd_, n->flush_period_secs_, n->flush_period_nsecs_);

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

        constexpr uint num_peers_init = (BASE_CLUSTER_SIZE / EVENT_LOOP_THREADS) + 1;

        for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
            std::optional<std::string> create_el_err = EventLoop<SOCKET_TYPE>::CreateEventLoop(
                &n->loops_[i], el_inbox, i, num_peers_init, n->event_fd_
            );
            if (create_el_err) {
                return (
                    std::format("error creating event loop {}:\n{}\n", i, create_el_err.value())
                );
            }
        }

        const char* init_cluster[BASE_CLUSTER_SIZE];
        setup_peers(init_cluster);

        for (int i = 0; i < BASE_CLUSTER_SIZE; ++i) {
            n->node_ids_.set_cluster_node(i);
            if (i == MY_ID) continue;

            auto result = encode(init_cluster[i], SERVER_PORT);
            if (std::holds_alternative<const char*>(result)) {
                return std::get<const char*>(result);
            }
            IPAddrPort ip_addr = std::get<IPAddrPort>(result);
            std::optional<std::string> add_peer_err = n->loops_[i & (EVENT_LOOP_THREADS - 1)]
                .AddPeer(i, ip_addr);
            if (add_peer_err) {
                return (
                    std::format("error creating node:\n{}\n", add_peer_err.value())
                );
            }
            n->node_ids_.set_online_node(i);
        }

        for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
            n->threads_[i] = std::jthread([n, i] {
                std::optional<std::string> loop_err = n->loops_[i].Run();
                #ifdef DEBUG
                if (loop_err) {
                    std::cout << "event loop " << i << " crashed:\n" << loop_err.value() << "\n";
                }
                #endif
            });
        }

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

        return {};
}

inline Node::~Node() {
    running_ = false;
    for (uint i = 0; i < EVENT_LOOP_THREADS; ++i) {
        if (!loops_[i].stopped.load(std::memory_order_acquire)) loops_[i].Stop();
        if (threads_[i].joinable()) threads_[i].join();
    }

    if (log_fp_ != nullptr) ::fclose(log_fp_);
    if (snapshot_fp_ != nullptr) ::fclose(snapshot_fp_);
    if (snapshot_tmp_fp_ != nullptr) ::fclose(snapshot_tmp_fp_);

    ::close(event_fd_);
    ::close(election_timeout_fd_);
    ::close(heartbeat_fd_);
    ::close(flush_fd_);
}

inline void Node::Stop() {
    bool done = false;
    while (!done) {
        done = client_inbox_->PushOne(StopNodeMsg{});
    }
    wake_self();
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
    #ifdef DEBUG
    std::cout << "client request to append commands\n";
    #endif
    std::vector<LogEntry> entries;
    entries.reserve(commands.size());
    for (std::byte* command : commands) {
        entries.emplace_back(command, CMD_SIZE, 0); // term assigned on append
    }
    bool done = false;
    while (!done) {
        done = client_inbox_->PushOne(AppendClientReq{std::move(entries)});
    }
    wake_self();
}

inline void Node::append_commands(std::byte (&commands)[MAX_ENTRIES][CMD_SIZE], size_t num_entries) {
    std::vector<LogEntry> entries;
    entries.reserve(num_entries);
    for (size_t i = 0; i < num_entries; ++i) {
        entries.emplace_back(commands[i], CMD_SIZE, 0); // term assigned on append
    }
    bool done = false;
    while (!done) {
        done = client_inbox_->PushOne(AppendClientReq{std::move(entries)});
    }
    wake_self();
}

inline void Node::append_commands(std::vector<LogEntry>&& commands) {
    bool done = false;
    while (!done) {
        done = client_inbox_->PushOne(AppendClientReq{std::move(commands)});
    }
    wake_self();
}

inline void Node::append_commands_local(std::vector<LogEntry>&& commands) {
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
        return; // note: nodes that do not know who the leader is will drop these entries
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

    const size_t num_entries = commands.size();
    log_.reserve(log_.size() + num_entries);
    for (LogEntry& e : commands) {
        e.term = current_term_;
        log_.push_back(std::move(e));
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

inline void Node::forward_request(const std::vector<LogEntry>& commands) {
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
            std::memcpy(msg.entries[i], commands[sent + i].data_, CMD_SIZE);
        }
        el.outbound_inbox.PushOne(EventLoopMessage(std::move(msg)));
        sent += num_entries;
    }
    el.Wake();
    return;
}

inline void Node::read_state(FILE* out) {
    #ifdef DEBUG
    std::cout << "Found client request to read state\n";
    #endif
    bool done = false;
    while (!done) {
        done = client_inbox_->PushOne(ReadStateClientReq{out});
    }
    wake_self();
}

inline int Node::get_leader() {
    return leader_id_;
}

inline std::optional<const char*> Node::register_fd(FD fd, uint32_t events) {
    epoll_event ev{};
    ev.events  = events;
    ev.data.fd = fd;

    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        return ("epoll_ctl ADD failed");
    }
    return {};
}

inline std::optional<const char*> Node::set_timer_periodic(FD fd, uint64_t secs, uint64_t nsecs) {
    itimerspec spec{};
    spec.it_value.tv_sec  = secs;
    spec.it_value.tv_nsec = nsecs;
    spec.it_interval      = spec.it_value;
    ::timerfd_settime(fd, 0, &spec, nullptr);
    return {};
}

inline std::optional<const char*> Node::set_timer(FD fd, uint64_t secs, uint64_t nsecs) {
    itimerspec spec{};
    spec.it_value.tv_sec  = secs;
    spec.it_value.tv_nsec = nsecs;
    spec.it_interval      = {0, 0};
    ::timerfd_settime(fd, 0, &spec, nullptr);
    return {};
}

inline std::optional<const char*> Node::reset_timer(FD fd, uint64_t secs, uint64_t nsecs) {
    uint64_t expirations = 0;
    ssize_t n = ::read(fd, &expirations, sizeof(expirations));
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return "error attempting to read fd\n";
    }
    // if (n != sizeof(expirations) || expirations == 0) return {};

    return set_timer(fd, secs, nsecs);
}

inline void Node::randomize_election_timeout() {
    const uint64_t election_timeout_ns = distrib_(rand_gen_);
    election_timeout_secs_ = election_timeout_ns / NS_PER_SEC;
    election_timeout_nsecs_ = election_timeout_ns % NS_PER_SEC;
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

inline std::optional<const char*> Node::demote() {
    #ifdef DEBUG
    std::cout << "this node (id " << MY_ID << ") was demoted\n";
    #endif

    NodeState old = state_;
    if (old == NodeState::Follower) return {};
    state_ = NodeState::Follower;
    for (size_t i = 0; i < next_indexes_.size(); ++i) {
        if (next_indexes_[i] < 0) continue;
        next_indexes_[i] = static_cast<uint32_t>(log_.size()) + base_logical_idx_; // last log idx + 1
        match_indexes_[i] = 0;
        chunks_sent_[i] = 0;
    }
    if (old == NodeState::Candidate) return {};
    installing_snapshot_.reset();

    reset_timer(heartbeat_fd_, 0, 0);

    return {};
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

    set_timer_periodic(heartbeat_fd_, heartbeat_period_secs_, heartbeat_period_nsecs_);
}

inline void Node::add_peer_if_not_exists(NodeID node_id, IPAddrPort ip_addr, EventLoop<SOCKET_TYPE>& el) {
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
            AddPeerMsg{ .ip_addr = ip_addr, .dest_id = node_id }
        )
    );

    el.Wake();
    #ifdef DEBUG
    std::cout << "added node w/ id " << node_id << " to node_ids_\n";
    #endif
}

inline uint32_t Node::compute_new_commit_idx() {
    if (log_.empty()) return commit_index_;

    const uint32_t last_log_idx = (log_.size() - 1) + base_logical_idx_;
    #ifdef DEBUG
    std::cout << "last log idx = " << last_log_idx << "\n";
    #endif

    const size_t majority = (node_ids_.num_in_cluster - 1) / 2;

    // every member's last-known match index; self is always last_log_idx
    std::array<int, MAX_NODES> matches{};
    int idx = 0;
    for (int m : match_indexes_) {
        if (m < 0) continue;
        matches[idx++] = m;
    }
    matches[idx++] = last_log_idx;
    if (majority >= idx) return commit_index_;
    std::nth_element(matches.begin(), matches.begin() + majority, matches.begin() + idx);

    #ifdef DEBUG
    std::cout << "Matches:\n";
    for (auto x : matches) {
        std::cout << x << ", ";
    }
    std::cout << "\n";
    #endif

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

    if (last_applied_idx_ != commit_index_) {
        last_applied_idx_ = commit_index_;
        last_applied_term_ = commit_index_ < base_logical_idx_ ? base_term_ : log_[commit_index_ - base_logical_idx_].term;
        #ifdef DEBUG
        std::cout << "last_applied_idx_ set to " << last_applied_idx_ << "\n";
        std::cout << "last_applied_term_ set to " << last_applied_term_ << "\n";
        #endif
    }
}

inline std::optional<std::string> Node::compact() {
    #ifdef DEBUG
    std::cout << "log size reached compact threshold; compacting...\n";
    std::cout << "log_.size() = " << log_.size() << "\n";
    std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
    std::cout << "base_logical_idx_ = " << base_logical_idx_ << "\n";
    std::cout << "commit_index_ = " << commit_index_ << "\n";
    #endif

    const size_t num_applied = last_applied_idx_ - base_logical_idx_ + 1;

    #ifdef DEBUG
    std::cout << "num_applied = " << num_applied << "\n";
    #endif

    base_term_ = log_[num_applied - 1].term;

    #ifdef DEBUG
    std::cout << "base_term_ = " << base_term_ << "\n";
    #endif

    FILE* sm_tmp_fp = ::fopen(STATE_MACHINE_TMP_FILE_PATH, "w+");
    if (sm_tmp_fp == nullptr) {
        return (std::format(
            "Error opening state machine tmp file with path {}\n",
            STATE_MACHINE_TMP_FILE_PATH
        ));
    }

    reconstruct_state(sm_tmp_fp, last_applied_idx_);
    log_.erase(log_.begin(), log_.begin() + num_applied);
    base_logical_idx_ = last_applied_idx_ + 1;

    #ifdef DEBUG
    std::cout << "base_logical_idx_ = " << base_logical_idx_ << "\n";
    std::cout << "log_.size() after compaction = " << log_.size() << "\n";
    #endif

    ::freopen(LOG_FILE_PATH, "w+", log_fp_);
    ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
    ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
    // preserve any unapplied entries so they can be replayed after a restart
    if (!log_.empty()) {
        ::fwrite(log_.data(), sizeof(LogEntry), log_.size(), log_fp_);
    }

    // create the snapshot and write to disk
    ::fflush(sm_tmp_fp);
    struct stat st;
    if (fstat(fileno(sm_tmp_fp), &st) != 0) {
        return "compaction failed: failed to get SM temp file size\n";
    }
    snapshot_tmp_fp_ = ::fopen(SNAPSHOT_TMP_FILE_PATH, "w+");
    const size_t cluster_size_bytes = node_ids_.bytes();
    ::fwrite(&last_applied_idx_, sizeof(last_applied_idx_), 1, snapshot_tmp_fp_);
    ::fwrite(&last_applied_term_, sizeof(last_applied_term_), 1, snapshot_tmp_fp_);

    __off_t sm_offset{0};
    __off64_t snapshot_offset{static_cast<__off64_t>(snapshot_config_and_data_offset_bytes())};
    ssize_t n = copy_file_range(fileno(sm_tmp_fp), &sm_offset,
                                fileno(snapshot_tmp_fp_), &snapshot_offset,
                                st.st_size, 0);
    if (n != st.st_size) {
        return std::format(
            "failed to compact - copy_file_range failed to copy all bytes from SM to snapshot file (n = {}, SM file size = {})\n",
            n, st.st_size
        );
    }

    ::fclose(sm_tmp_fp);

    // commit the temp file to disk
    ::fflush(snapshot_tmp_fp_);
    ::fsync(fileno(snapshot_tmp_fp_));
    ::fclose(snapshot_tmp_fp_);
    if (snapshot_fp_ != nullptr) ::fclose(snapshot_fp_);
    snapshot_tmp_fp_ = nullptr;
    ::rename(SNAPSHOT_TMP_FILE_PATH, SNAPSHOT_FILE_PATH);
    snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");

    return {};
}

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
        node_ids_.set_unavailable_node(MY_ID);

        #ifdef DEBUG
        std::cout << "---RECOVERY---:\n";
        std::cout << "last included index = " << last_applied_idx_ << "\n";
        std::cout << "last included term = " << last_applied_term_ << "\n";
        std::cout << "cluster size bytes = " << cluster_size_bytes << "\n";
        print_cluster();
        std::cout << "------\n";
        #endif

        ::fclose(snapshot_fp_);
        snapshot_fp_ = nullptr;
        snapshot_restored = true;
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

    // Open snapshot file for runtime use
    snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, snapshot_restored ? "r+" : "w+");
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

inline std::optional<std::string> Node::reconstruct_state(FILE* out, uint32_t up_to_idx) {
    assert(snapshot_fp_ != nullptr);

    struct stat snapshot_stat;
    ::fseek(out, 0, SEEK_SET);
    if (stat(SNAPSHOT_FILE_PATH, &snapshot_stat) == 0
        && snapshot_stat.st_size >= static_cast<off_t>(snapshot_header_bytes())) {
            __off64_t zero{0};
            __off64_t snapshot_offset{static_cast<__off64_t>(snapshot_config_and_data_offset_bytes())};
            ssize_t n = copy_file_range(fileno(snapshot_fp_), &snapshot_offset,
                                        fileno(out), &zero,
                                        snapshot_stat.st_size - snapshot_config_and_data_offset_bytes(), 0);
            if (n != snapshot_stat.st_size - snapshot_config_and_data_offset_bytes()) {
                return std::format(
                    "failed to reconstruct state - copy_file_range failed to copy all bytes from snapshot file (n = {}, snapshot file size = {})\n",
                    n, snapshot_stat.st_size
                );
            }

    }
    else {
        const size_t cluster_size_bytes = node_ids_.bytes();
        ::fwrite(&cluster_size_bytes, sizeof(cluster_size_bytes), 1, out);
        ::fwrite(node_ids_.cluster.data(), cluster_size_bytes, 1, out);
    }

    for (uint32_t i = base_logical_idx_; i <= up_to_idx; ++i) {
        #ifdef DEBUG
        std::cout << "in state machine: applying entry at logical index " << i << "\n";
        #endif
        apply_entry(out, log_[i - base_logical_idx_]);
        //::fseek(out, sm_header_bytes(), SEEK_SET);
    }

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

inline std::optional<std::string> Node::send_append_entries(int32_t next_idx, EventLoop<SOCKET_TYPE>& el, NodeID dest_id) {
    #ifdef DEBUG
    std::cout << "checking for entries to send to node " << dest_id << "\n";
    std::cout << "next index = " << next_idx << "\n";
    std::cout << "base logical idx = " << base_logical_idx_ << "\n";
    std::cout << "log_.size() == " << log_.size() << "\n";
    #endif
    if (next_idx <= 0) {
        return {};
    }
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

inline void Node::wake_self() {
    #ifdef DEBUG
    std::cout << "Main thread - waking node\n";
    #endif
    uint64_t one = 1;
    ssize_t n = ::write(event_fd_, &one, sizeof(one));
    (void)n;
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

inline std::optional<std::string> Node::send_install_snapshot(EventLoop<SOCKET_TYPE>& el, NodeID dest_id) {
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
    iterate_node_ids([](size_t id){ // iterates through online (reachable) nodes only
        std::cout << id << ", ";
    });

    std::cout << "\n";
}

#include "./main_loop_v2.hpp"
