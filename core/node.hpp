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
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <array>
#include <unordered_set>
#include <vector>

struct Node {
public:
    static VoidExpectedF CreateNode(Node*, NodeInbox*, void(*)(FILE*, const LogEntry&), void(*)(FILE*, FILE*));
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
    void append_commands(std::byte (&)[CMD_SIZE][MAX_ENTRIES], size_t num_entries);

    void forward_request(std::vector<std::byte*>&);
    void forward_request(std::byte (&)[CMD_SIZE][MAX_ENTRIES], size_t num_entries);

    NodeID get_leader();
    // TODO: replace AoS EventLoop w/ SoA pattern
    private:
    template <typename T>
    void send(T&& payload, EventLoop& el) {
        el.outbound_inbox.PushOne(
            RpcMessage(std::forward<T>(payload))
        );
        el.Wake();
    }
    VoidExpectedF send_append_entries(uint32_t next_idx, EventLoop&, NodeID);
    VoidExpectedF send_install_snapshot(EventLoop&, NodeID);
    void request_votes();

    void demote();
    void become_leader();

    void add_peer_if_not_exists(NodeID, FD, EventLoop&);
    uint32_t compute_new_commit_idx();
    void commit_entries_if_available();

    /* log compaction/snapshotting/recovery */
    VoidExpectedF recover();

    void write_current_term();
    void write_voted_for();
    void flush_files();

    std::unordered_set<NodeID>                                      voters_;
    std::vector<LogEntry>                                           log_;
    std::vector<std::chrono::steady_clock::time_point>              last_ae_sent_            = std::vector<std::chrono::steady_clock::time_point>(BASE_CLUSTER_SIZE, std::chrono::steady_clock::time_point::max());
    std::vector<std::chrono::steady_clock::time_point>              last_rv_sent_            = std::vector<std::chrono::steady_clock::time_point>(BASE_CLUSTER_SIZE, std::chrono::steady_clock::time_point::max());
    std::vector<std::chrono::steady_clock::time_point>              last_is_sent_            = std::vector<std::chrono::steady_clock::time_point>(BASE_CLUSTER_SIZE, std::chrono::steady_clock::time_point::max());
    std::vector<size_t>                                             chunks_sent_             = std::vector<size_t>(BASE_CLUSTER_SIZE, 0); // after every IS RPC send, increment by 1
    std::vector<int32_t>                                            next_indexes_            = std::vector<int32_t>(BASE_CLUSTER_SIZE, 1);         // leader-only, one per peer
    std::vector<int32_t>                                            match_indexes_           = std::vector<int32_t>(BASE_CLUSTER_SIZE, 0);         // leader-only, one per peer
    DynamicBitset                                                   node_ids_                = DynamicBitset(BASE_CLUSTER_SIZE);

    std::array<EventLoop, EVENT_LOOP_THREADS>                       loops_{};
    std::array<std::thread, EVENT_LOOP_THREADS>                     threads_;

    std::chrono::steady_clock::time_point                           last_leader_contact_;
    std::chrono::steady_clock::time_point                           last_flush_;
    std::chrono::milliseconds                                       election_timeout_;     // Election timeout, randomized at construction.
    NodeInbox*                                                      inbox_;
    FILE*                                                           log_fp_                  = nullptr;
    FILE*                                                           snapshot_fp_             = nullptr;
    FILE*                                                           snapshot_tmp_fp_         = nullptr;
    FILE*                                                           sm_fp_                   = nullptr;
    void(*apply_entry)(FILE*, const LogEntry&);
    void(*create_snapshot)(FILE*, FILE*);
    int                                                             leader_id_               = -1;
    int                                                             installing_snapshot_id_  = -1;
    int                                                             voted_for_               = -1;
    uint32_t                                                        last_applied_idx_        = 0;
    uint32_t                                                        last_applied_term_       = 0;
    uint32_t                                                        last_applied_idx_ss_     = 0;
    uint32_t                                                        last_applied_term_ss_    = 0;
    uint32_t                                                        current_term_            = 0;
    uint32_t                                                        commit_index_            = 0;     // index of highest log entry known to be committed
    enum class                                                      NodeState { Follower, Candidate, Leader };
    NodeState                                                       state_                   = NodeState::Follower;
    bool                                                            running_                 = false;
};
