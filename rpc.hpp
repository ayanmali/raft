#include <utility>

struct AppendEntriesRPC {
    const std::vector<int>& entries;
    int term;
    int leader_id;
    int prev_log_idx;
    int prev_log_term;
    int leader_commit;
};

struct RequestVoteRPC {
    int term;
    int candidate_id;
    int last_log_idx;
    int last_log_term;
};

struct InstallSnapshotRPC {
    const std::vector<std::byte>& data;
    int term;
    int leader_id;
    int last_included_idx;
    int last_included_term;
    int offset;
    bool done; // true if this is the last chunk
};

// returns term, success
inline std::pair<int, bool> SendAppendEntriesRPC(const AppendEntriesRPC& payload) {
    auto result = std::pair<int, bool>();
    return result;
};

// returns term, vote_granted
inline std::pair<int, bool> SendRequestVoteRPC(const RequestVoteRPC& payload) {
    auto result = std::pair<int, bool>();
    return result;
}

// returns current term #, for leader to update
inline int SendInstallSnapshotRPC(const InstallSnapshotRPC& payload) {
    int result;
    return result;
}