#include <utility>
#include "../node.hpp"

// returns term, success
inline std::pair<int, bool> Node::send_append_entries_rpc(const AppendEntriesPayload& payload) {
    auto result = std::pair<int, bool>();
    return result;
};

// returns term, vote_granted
inline std::pair<int, bool> Node::send_request_vote_rpc(const RequestVotePayload& payload) {
    auto result = std::pair<int, bool>();
    return result;
}

// returns current term #, for leader to update
inline int Node::send_install_snapshot_rpc(const InstallSnapshotPayload& payload) {
    int result;
    return result;
}