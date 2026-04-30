#pragma once
/*
RPC payload structs.

Split out so that protocol.hpp (the wire-format ser/de helpers) and
rpc.hpp (the Node::send_*_rpc bodies) can share these types without
forming an include cycle.
*/
#include <cstddef>
#include <cstdint>
#include <vector>

struct AppendEntriesReqPayload {
    std::vector<std::byte> entries;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t prev_log_idx;
    uint32_t prev_log_term;
    uint32_t leader_commit;
};

struct AppendEntriesRespPayload {
    uint32_t term;
    uint8_t success; // 0 = fail, 1 = success
};

struct RequestVoteReqPayload {
    uint32_t term;
    uint32_t candidate_id;
    uint32_t last_log_idx;
    uint32_t last_log_term;
};

struct RequestVoteRespPayload {
    uint32_t term;
    uint8_t vote_granted;
};

struct InstallSnapshotReqPayload {
    std::vector<std::byte> snapshot;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t last_included_idx;
    uint32_t last_included_term;
    uint32_t offset;
    uint8_t done; // non-zero if this is the last chunk
};

struct InstallSnapshotRespPayload {
    uint32_t term;
};
