#pragma once
/*
RPC request/response payload structs.
*/
#include "../../core/log_entry.hpp"
#include <cstddef>
#include <cstdint>
#include <vector>
#include <span>

static constexpr uint8_t AE_RPC_ID = 1;
static constexpr uint8_t RV_RPC_ID = 2;
static constexpr uint8_t IS_RPC_ID = 3;
static constexpr uint8_t SH_RPC_ID = 4;
static constexpr uint8_t AE_REPLY_ID = 5;
static constexpr uint8_t RV_REPLY_ID = 6;
static constexpr uint8_t IS_REPLY_ID = 7;

struct AppendEntriesReqPayload {
    std::vector<LogEntry> entries;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t prev_log_idx;
    uint32_t prev_log_term;
    uint32_t leader_commit;

    AppendEntriesReqPayload(std::vector<LogEntry> entries, uint32_t term, uint32_t leader_id, uint32_t prev_log_idx, uint32_t prev_log_term, uint32_t leader_commit) :
        entries(std::move(entries)),
        term(term),
        leader_id(leader_id),
        prev_log_idx(prev_log_idx),
        prev_log_term(prev_log_term),
        leader_commit(leader_commit)
    {};

    AppendEntriesReqPayload(std::span<const LogEntry> entries_span, uint32_t term, uint32_t leader_id, uint32_t prev_log_idx, uint32_t prev_log_term, uint32_t leader_commit) :
        entries(entries_span.begin(), entries_span.end()),
        term(term),
        leader_id(leader_id),
        prev_log_idx(prev_log_idx),
        prev_log_term(prev_log_term),
        leader_commit(leader_commit)
    {};

    AppendEntriesReqPayload(uint32_t term) : term(term) {};

    AppendEntriesReqPayload() {};

    auto size() const {
        size_t s = 0;
        for (const auto& entry : entries) {
            s += entry.data.size();
            s += sizeof(entry.term);
            s += sizeof(uint64_t); // for the length of entry.data itself in serialization
        }
        s += sizeof(term) + sizeof(leader_id) + sizeof(prev_log_idx) + sizeof(prev_log_term) + sizeof(leader_commit);
        s += sizeof(uint64_t); // for the number of entries
        return s;
    };
};

struct AppendEntriesRespPayload {
    uint32_t term;
    uint8_t success; // 0 = fail, 1 = success

    auto size() const {
      auto s = sizeof(term) + sizeof(success);
      return s;
    };
};

struct RequestVoteReqPayload {
    uint32_t term;
    uint32_t candidate_id;
    uint32_t last_log_idx;
    uint32_t last_log_term;

    RequestVoteReqPayload(uint32_t term, uint32_t candidate_id, uint32_t last_log_idx, uint32_t last_log_term)
    : term(term),
      candidate_id(candidate_id),
      last_log_idx(last_log_idx),
      last_log_term(last_log_term)
    {};

    RequestVoteReqPayload() {};

    auto size() const {
      auto s = sizeof(term) + sizeof(candidate_id) + sizeof(last_log_idx) + sizeof(last_log_term);
      return s;
    }
};

struct RequestVoteRespPayload {
    uint32_t term;
    uint8_t vote_granted;

    auto size() const {
      auto s = sizeof(term) + sizeof(vote_granted);
      return s;
    };
};

struct InstallSnapshotReqPayload {
    std::vector<std::byte> snapshot;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t last_included_idx;
    uint32_t last_included_term;
    uint32_t offset;
    uint8_t done; // non-zero if this is the last chunk

    InstallSnapshotReqPayload(std::vector<std::byte>& snapshot, uint32_t term, uint32_t leader_id, uint32_t last_included_idx, uint32_t last_included_term, uint32_t offset, uint8_t done)
    : snapshot(snapshot),
      term(term),
      leader_id(leader_id),
      last_included_idx(last_included_idx),
      last_included_term(last_included_term),
      offset(offset),
      done(done)
    {}

    InstallSnapshotReqPayload(std::vector<std::byte>&& snapshot, uint32_t term, uint32_t leader_id, uint32_t last_included_idx, uint32_t last_included_term, uint32_t offset, uint8_t done)
    : snapshot(std::move(snapshot)),
      term(term),
      leader_id(leader_id),
      last_included_idx(last_included_idx),
      last_included_term(last_included_term),
      offset(offset),
      done(done)
    {}

    InstallSnapshotReqPayload() {};

    auto size() const {
      auto s = snapshot.size() + sizeof(uint64_t) + sizeof(term) + sizeof(leader_id) + sizeof(last_included_idx) + sizeof(last_included_term) + sizeof(offset) + sizeof(done);
      return s;
    };

};

struct InstallSnapshotRespPayload {
    uint32_t term;

    auto size() const {
      auto s = sizeof(term);
      return s;
    }
};

struct ArmTimer {};
struct DisArmTimer {};

struct HeartbeatTimeoutPayload {};
