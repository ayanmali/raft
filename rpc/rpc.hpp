/*
Each node receives on two sockets:
- one for AE and RV RPCs
- one for IS RPCs
*/
#include <vector>
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