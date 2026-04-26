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
#include "./rpc/conn_pool.hpp"
#include <chrono>
#include <netdb.h>
#include <random>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

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
    void client_connect(int& sock_fd, std::string_view ip_addr, int port);
    void client_send(const char* msg, int len, int flags);
    void client_recv(int len, int flags);
    int client_close();

    // Server
    void server_bind();
    void server_listen();
    void server_accept();
    void server_reply();

    // returns term, success
    std::pair<int, bool> send_append_entries_rpc(const AppendEntriesPayload& payload);
    // returns term, vote_granted
    std::pair<int, bool> send_request_vote_rpc(const RequestVotePayload& payload);
    // returns current term #, for leader to update
    int send_install_snapshot_rpc(const InstallSnapshotPayload& payload);

    void loop();

    private:
    ConnectionPool connections;
    //std::vector<std::string_view> peers(std::move(init_peers)); // nodes discover each other via a "gossip"-like protocol
    char sock_buf[MAX_BUFFER_SIZE];
    struct addrinfo* res = nullptr; // used for client connections; freed in the destructor to avoid extra latency in the hotpath

    std::vector<LogEntry> log;
    std::vector<int> next_index; // one for each peer
    std::vector<int> match_index; // one for each peer

    std::chrono::milliseconds timeout; // randomly chosen from 150-300 ms

    // sockets
    int server_fd; // for receiving AE and RV RPCs
    // int server_snapshot_fd; // for receiving InstallSnapshot RPCs
    int client_fd; // for sending AE and RV RPCs
    // int client_snapshot_fd; // for sending InstallSnapshot RPCs
    bool leader;

    int current_term;
    int voted_for;
    int commit_index;
    int last_applied;
};

inline Node::Node() : connections(MAX_POOL_SIZE) {

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
    freeaddrinfo(res);
    res = nullptr;

    close(client_fd);
    close(server_fd);
    connections.~ConnectionPool();
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