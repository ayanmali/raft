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
#include "./rpc/rpc.hpp"
#include "./config.hpp"
#include <chrono>
#include <random>
#include <sys/socket.h>
#include <sys/types.h>

struct LogEntry {
    std::vector<std::byte> data;
    int term;
};

struct Node {
    public:
    Node();

    void increment_current_term();
    void set_voted_for(int voted_for);
    void append_to_log(std::string_view entry);

    void send_heartbeats(); // send AE RPCs w/ no log entries
    void start_election(); // enter candidate mode
    void compact_log();

    // returns term, success
    std::pair<int, bool> send_append_entries_rpc(const AppendEntriesPayload& payload);
    // returns term, vote_granted
    std::pair<int, bool> send_request_vote_rpc(const RequestVotePayload& payload);
    // returns current term #, for leader to update
    int send_install_snapshot_rpc(const InstallSnapshotPayload& payload);

    void loop();

    private:
    std::vector<std::string_view> peers(std::move(init_peers)); // nodes discover each other via a "gossip"-like protocol
    std::vector<LogEntry> log;
    std::vector<int> next_index; // one for each server
    std::vector<int> match_index; // one for each server

    std::chrono::milliseconds timeout; // randomly chosen from 150-300 ms

    // sockets
    int server_fd; // for receiving AE and RV RPCs
    int server_snapshot_fd; // for receiving InstallSnapshot RPCs
    int client_fd; // for sending AE and RV RPCs
    int client_snapshot_fd; // for sending InstallSnapshot RPCs
    int setup_socket(int& sock_fd, int port) { 
        sock_fd = socket("AF_INET", SOCK_STREAM, 0);
        if (sock_fd == -1) {
            return -1;
        }
        struct sockaddr_in sock_addr;
        sock_addr.sin_family = AF_INET;
        sock_addr.sin_port = htons(port);
        sock_addr.sin_addr.s_addr = htonl(INADDR_ANY);
        std::memset(&(sock_addr.sin_zero), '\0', 8); // zero the rest of the struct
        
        int sock_bind = bind(
            sock_fd, 
            (struct sockaddr*)&sock_addr,
            sizeof(sockaddr)
        );
        if (sock_bind == -1) {
            return -1;
        }
        return 0;
    }

    int current_term;
    int voted_for;
    int commit_index;
    int last_applied;
};

inline Node::Node() {
    // initialize timeout
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS);
    timeout = std::chrono::milliseconds(distrib(gen));

    // set up sockets
    int err;
    err = setup_socket(server_fd, SERVER_PORT);
    if (err != 0) {
        // handle error
    }

    err = setup_socket(server_snapshot_fd, SERVER_SNAPSHOT_PORT);
    if (err != 0) {
        // handle error
    }

    err = setup_socket(client_fd, CLIENT_PORT);
    if (err != 0) {
        // handle error
    }

    err = setup_socket(client_snapshot_fd, CLIENT_SNAPSHOT_PORT);
    if (err != 0) {
        // handle error
    }

};

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
    // after timeout, trigger an election
    start_election();
}