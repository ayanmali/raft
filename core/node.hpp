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
#include "../rpc/event_loop/event_loop.hpp"
#include "../rpc/protocol/payloads.hpp"
#include "log_entry.hpp"
#include <array>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>

struct Node {
public:
    //Node(NodeInbox&);
    static std::expected<std::unique_ptr<Node>, std::string> CreateNode(NodeInbox&);
    ~Node();

    Node(const Node&)            = delete;
    Node& operator=(const Node&) = delete;
    Node(Node&&)                 = delete;
    Node& operator=(Node&&)      = delete;

    // Signals every loop to exit, joins all worker threads.
    void Stop();

    void MainLoop();

    // Outbound RPC entry points. Resolve the owning loop by
    // peer_id % N and post the request into its inbox. The reply
    // callback fires on that loop's thread.
    void send_rpc(AppendEntriesReqPayload&&, NodeID peer_id);
    void send_rpc(RequestVoteReqPayload&&, NodeID peer_id);
    void send_rpc(InstallSnapshotReqPayload&&, NodeID peer_id);
    void append_log_entries(std::vector<std::vector<std::byte>>& entry);
    void send_heartbeats_and_arm_timers(); // upon leader promotion
    void send_disarm_timers(); // upon leader demotion

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

    // RpcReply handle_request(const RpcRequest& message);

    // AppendEntriesRespPayload     handle_append_entries(const AppendEntriesReqPayload&);
    // RequestVoteRespPayload       handle_request_vote(const RequestVoteReqPayload&);
    // InstallSnapshotRespPayload   handle_install_snapshot(const InstallSnapshotReqPayload&);

    private:
    NodeInbox& inbox;
    // ---- transport ----
    // TODO: replace AoS EventLoop w/ SoA pattern
    std::vector<NodeID>                                             node_ids;
    std::unordered_set<NodeID>                                      voters;
    std::array<std::unique_ptr<EventLoop>, EVENT_LOOP_THREADS>      loops_;
    std::array<std::thread, EVENT_LOOP_THREADS>                     threads_;
    std::chrono::steady_clock::time_point                           last_leader_contact;
    bool                                                            running_ = false;

    // ---- raft state ----
    uint32_t                                       current_term  = 0;
    int                                            voted_for     = -1;
    std::vector<LogEntry>                          log;
    uint32_t                                       commit_index  = 0;   // index of highest log entry known to be committed
    uint32_t                                       last_applied  = 0;   // index of highest log entry applied to state machine
    std::vector<int>                               next_index;          // leader-only, one per peer
    std::vector<int>                               match_index;         // leader-only, one per peer
    enum class                                     NodeState { Follower, Candidate, Leader };
    NodeState                                      state         = NodeState::Follower;
    // Election timeout, randomized at construction.
    std::chrono::milliseconds                      election_timeout_;

    // ---- election state ----
    uint32_t votes_received;

    // ---- setup helpers ----

    // Constructs the RpcHandlers struct (member-fn lambdas) the loops
    // dispatch to on inbound requests.
    // RpcHandlers make_handlers();
    //
    Node(NodeInbox&);
    //void tick_peer(NodeID peer_id);

    // std::expected<RpcReply, const char*> handle_append_entries_req(const RpcRequest& message);
    // std::expected<RpcReply, const char*> handle_request_vote_req(const RpcRequest& message);
    // std::expected<RpcReply, const char*> handle_install_snapshot_req(const RpcRequest& message);

};
