#pragma once
/*
On disk:
- currentTerm
- votedFor
- log[]

volatile (all servers):
- commitIndex
- lastApplied
volatile (leaders) (reinitialized after election):
- nextIndex[]
- matchIndex[]
*/
#include "./config.hpp"
#include "./rpc/conn_pool.hpp"
#include "rpc/client/conn_pool.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <netdb.h>
#include <random>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

// Forward-declared here; full definitions live in rpc/rpc.hpp.
// This avoids the include cycle node.hpp <-> rpc/rpc.hpp.
struct AppendEntriesReqPayload;
struct RequestVoteReqPayload;
struct InstallSnapshotReqPayload;
struct AppendEntriesRespPayload;
struct RequestVoteRespPayload;
struct InstallSnapshotRespPayload;

struct LogEntry {
    std::vector<std::byte> data;
    int term;
};

struct Node {
    public:
    Node();
    ~Node();

    void increment_current_term();
    void set_voted_for(int voted_for);
    void append_to_log(std::string_view entry);

    void send_heartbeats(); // send AE RPCs w/ no log entries
    void start_election(); // enter candidate mode
    void compact_log();

    // Calling `socket()`
    void setup_sockets();
    /* do not inline */
    void handle_setup_errs(std::initializer_list<int> sock_fds);

    // Client
    // Sends a length-prefixed request to ip:port and reads the
    // length-prefixed reply into out[0..out_cap). Returns the reply length
    // on success. Routes through the LRU connection pool: hit -> reuse fd,
    // miss -> connect + cache. On dead-connection errors, closes the fd,
    // drops the cache entry, reconnects, and retries exactly once.
    uint32_t client_request(std::string_view ip,
                            std::string_view port,
                            const std::byte* msg, uint32_t len,
                            std::byte* out, uint32_t out_cap);

    // Server
    // Binds, listens, and accepts
    void server_expose(const char* port);

    // returns term, success
    AppendEntriesRespPayload send_append_entries_rpc(std::string_view peer_ip,
                                                     const AppendEntriesReqPayload& payload);

    // returns term, vote_granted
    RequestVoteRespPayload send_request_vote_rpc(std::string_view peer_ip,
                                                 const RequestVoteReqPayload& payload);

    // returns current term #, for leader to update
    InstallSnapshotRespPayload send_install_snapshot_rpc(std::string_view peer_ip,
                                                         const InstallSnapshotReqPayload& payload);

    void loop();

    private:
    // client connections
    // server connections are maintained by the event loop struct
    ConnectionPool connections;

    std::vector<LogEntry> log;
    std::vector<int> next_index; // one for each peer
    std::vector<int> match_index; // one for each peer

    std::chrono::milliseconds timeout; // randomly chosen from 150-300 ms

    // Per-peer client fds live in `connections`. The server fd below is the
    // listening socket; per-connection fds for inbound RPCs are owned by
    // the event loop's ConnSlab.
    int  server_fd;
    bool leader;

    int current_term;
    int voted_for;
    int commit_index;
    int last_applied;

    // Resolves a writable fd for `peer_ip`: hits the connection pool, or
    // connects on miss and caches (closing any LRU-evicted fd).
    int peer_fd(std::string_view peer_ip);
    
    // Used in server_expose()
    void bind_and_listen(const char* port);
};

inline Node::Node() : connections(MAX_CLIENT_CONNS) {

    // initialize timeout
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS);
    timeout = std::chrono::milliseconds(distrib(gen));

    // calling `socket()`
    setup_sockets();

    // ...

};

inline Node::~Node() {
    connections.close_all();
    if (server_fd >= 0) close(server_fd);
}

inline void Node::start_election() {
    increment_current_term();
};

inline void Node::loop() {
    auto start = std::chrono::high_resolution_clock::now();
    auto end = start + timeout;
    while (std::chrono::high_resolution_clock::now() < end) {
        // spin
        // if an RPC is received, no election
        return;
    }
    // timeout exceeded; trigger an election
    start_election();
}