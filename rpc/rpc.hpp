/*

*/
#include "../node.hpp"
#include <vector>
#include <utility>

struct AppendEntriesPayload {
    const std::vector<std::byte>& entries;
    int term;
    int leader_id;
    int prev_log_idx;
    int prev_log_term;
    int leader_commit;
};

struct RequestVotePayload {
    int term;
    int candidate_id;
    int last_log_idx;
    int last_log_term;
};

struct InstallSnapshotPayload {
    const std::vector<std::byte>& data;
    int term;
    int leader_id;
    int last_included_idx;
    int last_included_term;
    int offset;
    bool done; // true if this is the last chunk
};


void Node::setup_sockets() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    // server_snapshot_fd = socket(AF_INET, SOCK_STREAM, 0);
    client_fd = socket(AF_INET, SOCK_STREAM, 0);
    // client_snapshot_fd = socket(AF_INET, SOCK_STREAM, 0);

    // handle_setup_errs({server_fd, server_snapshot_fd, client_snapshot_fd, client_snapshot_fd, });
    handle_setup_errs({server_fd, client_fd });
}

void Node::handle_setup_errs(std::initializer_list<int> sock_fds) {
    for (auto fd : sock_fds) {
        if (fd < 0) throw std::runtime_error("Error setting up socket fds");
    }
}

// returns term, success
std::pair<int, bool> Node::send_append_entries_rpc(const AppendEntriesPayload& payload) {
    auto result = std::pair<int, bool>();
    return result;
};

// returns term, vote_granted
std::pair<int, bool> Node::send_request_vote_rpc(const RequestVotePayload& payload) {
    auto result = std::pair<int, bool>();
    return result;
}

// returns current term #, for leader to update
int Node::send_install_snapshot_rpc(const InstallSnapshotPayload& payload) {
    int result;
    return result;
}