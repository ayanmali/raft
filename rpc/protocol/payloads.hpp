#pragma once
/*
RPC request/response payload structs.
*/
#include "../../config.hpp"
#include <cstring>
#include <netinet/in.h>
#include <variant>

// static constexpr uint8_t AE_RPC_ID = 0;
// static constexpr uint8_t RV_RPC_ID = 1;
// static constexpr uint8_t IS_RPC_ID = 2;
// static constexpr uint8_t FL_RPC_ID = 3;
// static constexpr uint8_t AE_REPLY_ID = 4;
// static constexpr uint8_t RV_REPLY_ID = 5;
// static constexpr uint8_t IS_REPLY_ID = 6;

using NodeID = uint32_t;
using FD = int;

struct LogEntry {
    std::byte data_[CMD_SIZE];
    uint32_t term;

    LogEntry() : data_{} {};
    LogEntry(uint32_t term) : data_{}, term(term) {};
    LogEntry(std::byte* buf, size_t size, uint32_t term) : term(term) {
        assert(size <= CMD_SIZE);
        std::memcpy(data_, buf, size);
    };
};

struct AppendEntriesReqPayload {
    LogEntry entries[MAX_ENTRIES];
    size_t entries_len;
    FD fd; // populated by the event loop on client read; not serialized across network
    NodeID dest_id; // for routing purposes only; not serialized across network
    uint32_t term;
    uint32_t leader_id;
    uint32_t prev_log_idx;
    uint32_t prev_log_term;
    uint32_t leader_commit;

    AppendEntriesReqPayload(size_t entries_len, NodeID dest_id, uint32_t term, uint32_t leader_id, uint32_t prev_log_idx, uint32_t prev_log_term, uint32_t leader_commit) :
        entries_len(entries_len),
        dest_id(dest_id),
        term(term),
        leader_id(leader_id),
        prev_log_idx(prev_log_idx),
        prev_log_term(prev_log_term),
        leader_commit(leader_commit) {};

    AppendEntriesReqPayload() {};

    static constexpr auto size() {
        size_t s = sizeof(entries) + sizeof(entries_len) + sizeof(term) + sizeof(leader_id) + sizeof(prev_log_idx) + sizeof(prev_log_term) + sizeof(leader_commit);
        return s;
    };
};

struct AppendEntriesRespPayload {
    uint64_t entries_len;
    FD client_fd; // not serialized across network; only used for routing purposes
    NodeID server_id;
    uint32_t term;
    uint8_t success; // 0 = fail, 1 = success

    static constexpr auto size() {
      auto s = sizeof(server_id) + sizeof(entries_len) + sizeof(term) + sizeof(success);
      return s;
    };
};

struct RequestVoteReqPayload {
    FD fd; // populated by the event loop on client read; not serialized across network
    NodeID dest_id; // for routing purposes only; not serialized across network
    uint32_t term;
    uint32_t candidate_id;
    uint32_t last_log_idx;
    uint32_t last_log_term;

    // RequestVoteReqPayload(NodeID node_id, uint32_t term, uint32_t candidate_id, uint32_t last_log_idx, uint32_t last_log_term)
    // : node_id(node_id),
    //   term(term),
    //   candidate_id(candidate_id),
    //   last_log_idx(last_log_idx),
    //   last_log_term(last_log_term)
    // {};

    // RequestVoteReqPayload() {};

    static constexpr auto size() {
        auto s = sizeof(term) + sizeof(candidate_id) + sizeof(last_log_idx) + sizeof(last_log_term);
        return s;
    }
};

struct RequestVoteRespPayload {
    FD client_fd; // not serialized across network; only used for routing purposes
    NodeID server_id;
    uint32_t term;
    uint8_t vote_granted;

    static constexpr auto size() {
        auto s = sizeof(server_id) + sizeof(term) + sizeof(vote_granted);
        return s;
    };
};

struct InstallSnapshotReqPayload {
    std::byte partial_state[SNAPSHOT_CHUNK_SIZE]; // IS RPCs send smaller chunks of the state at a time
    size_t cluster_raw_size;
    uint8_t cluster[(MAX_NODES + BITS_PER_BYTE - 1) / BITS_PER_BYTE];
    uint32_t last_included_idx;
    uint32_t last_included_term;
    uint64_t offset;
    FD fd; // populated by the event loop on client read; not serialized across network
    NodeID dest_id; // for routing purposes only; populated by the event loop
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint8_t done; // non-zero if this is the last chunk

    // InstallSnapshotReqPayload(std::vector<std::byte>& snapshot, NodeID node_id, uint32_t term, uint32_t leader_id, uint32_t last_included_idx, uint32_t last_included_term, uint32_t offset, uint8_t done)
    // : snapshot(snapshot),
    //   dest_id(dest_id),
    //   term(term),
    //   leader_id(leader_id),
    //   last_included_idx(last_included_idx),
    //   last_included_term(last_included_term),
    //   offset(offset),
    //   done(done)
    // {}

    // InstallSnapshotReqPayload(std::vector<std::byte>&& snapshot, NodeID node_id, uint32_t term, uint32_t leader_id, uint32_t last_included_idx, uint32_t last_included_term, uint32_t offset, uint8_t done)
    // : snapshot(std::move(snapshot)),
    //   node_id(node_id),
    //   term(term),
    //   leader_id(leader_id),
    //   last_included_idx(last_included_idx),
    //   last_included_term(last_included_term),
    //   offset(offset),
    //   done(done)
    // {}

    // InstallSnapshotReqPayload() {};

    static constexpr auto size() {
        auto s = sizeof(partial_state) + sizeof(cluster_raw_size) + sizeof(cluster) + sizeof(last_included_idx) + sizeof(last_included_term) + sizeof(term) + sizeof(leader_id) + sizeof(offset) + sizeof(done);
        return s;
    };

};

struct InstallSnapshotRespPayload {
    FD client_fd; // not serialized across network; only used for routing purposes
    NodeID server_id;
    uint32_t term;

    static constexpr auto size() {
      auto s = sizeof(server_id) + sizeof(term);
      return s;
    }
};

struct ArmTimer { NodeID dest_id; };
struct DisarmTimer { NodeID dest_id; };

struct HeartbeatTimeout { NodeID source_id; };

/* For supporting dynamic cluster configurations */
struct DropPeerMsg { NodeID source_id; };
struct AddPeerMsg { FD fd; const char* port; NodeID dest_id; };
struct ForwardLeaderMsg {
    std::byte entries[CMD_SIZE][MAX_ENTRIES];
    size_t entries_len;
    FD fd; // populated by the event loop on client read; not serialized across network
    NodeID sender_id;
    NodeID dest_id; // for routing purposes only; not serialized across network
    uint32_t term;

    static constexpr auto size() {
        size_t s = sizeof(entries) + sizeof(entries_len) + sizeof(sender_id) + sizeof(term);
        return s;
    }
};

struct StopNodeMsg {};

struct AETimeout { NodeID source_id; };
struct RVTimeout { NodeID source_id; };
struct ISTimeout { NodeID source_id; };

using NodeMessage = std::variant<AppendEntriesReqPayload, RequestVoteReqPayload, InstallSnapshotReqPayload, AppendEntriesRespPayload, RequestVoteRespPayload, InstallSnapshotRespPayload, HeartbeatTimeout, DropPeerMsg, ForwardLeaderMsg, StopNodeMsg, AETimeout, RVTimeout, ISTimeout>;
using EventLoopMessage = std::variant<AppendEntriesReqPayload, RequestVoteReqPayload, InstallSnapshotReqPayload, ArmTimer, DisarmTimer, AppendEntriesRespPayload, RequestVoteRespPayload, InstallSnapshotRespPayload, AddPeerMsg, ForwardLeaderMsg>;
