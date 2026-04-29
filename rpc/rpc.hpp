#pragma once
/*

*/
#include "../node.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>
#include <vector>

struct AppendEntriesPayload {
    std::vector<std::byte> entries;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t prev_log_idx;
    uint32_t prev_log_term;
    uint32_t leader_commit;
};

struct RequestVotePayload {
    uint32_t term;
    uint32_t candidate_id;
    uint32_t last_log_idx;
    uint32_t last_log_term;
};

struct InstallSnapshotPayload {
    std::vector<std::byte> snapshot;
    uint32_t term;
    uint32_t leader_id; // 0 is null value
    uint32_t last_included_idx;
    uint32_t last_included_term;
    uint32_t offset;
    uint8_t done; // non-zero if this is the last chunk
};

inline void Node::setup_sockets() {
    // Only the listening socket is created here. Per-peer client sockets are
    // created lazily by client_request() and cached in `connections`.
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    handle_setup_errs({server_fd});
}

inline void Node::handle_setup_errs(std::initializer_list<int> sock_fds) {
    for (auto fd : sock_fds) {
        if (fd < 0) throw std::runtime_error("Error setting up socket fds");
    }
}

// Wire-format note: these helpers pack the POD fields of each payload into
// a fixed-size header and call client_request. The server's handler does
// not yet parse this; once it does, AppendEntries / InstallSnapshot will
// also need the variable-length entries/data appended after the header.
// TODO(rpc-format): finalize the wire format and add length-prefixed
// trailing payload sections.

// Helper: pack fields with memcpy into a tightly-packed byte buffer.
// ...

// returns term, success
inline std::pair<int, bool> Node::send_append_entries_rpc(std::string_view peer_ip,
                                                          const AppendEntriesPayload& payload) {
    std::byte buf[64];
    auto* p = buf;
    p = detail::pack(p, payload.term);
    p = detail::pack(p, payload.leader_id);
    p = detail::pack(p, payload.prev_log_idx);
    p = detail::pack(p, payload.prev_log_term);
    p = detail::pack(p, payload.leader_commit);
    const uint32_t req_len = static_cast<uint32_t>(p - buf);

    std::byte reply[INLINE_PAYLOAD_BYTES];
    uint32_t n = client_request(peer_ip, SERVER_PORT, buf, req_len,
                                reply, sizeof(reply));

    int  term    = 0;
    bool success = false;
    if (n >= sizeof(int) + sizeof(bool)) {
        const std::byte* q = reply;
        q = detail::unpack(q, &term);
        q = detail::unpack(q, &success);
    }
    return {term, success};
}

// returns term, vote_granted
inline std::pair<int, bool> Node::send_request_vote_rpc(std::string_view peer_ip,
                                                        const RequestVotePayload& payload) {
    std::byte buf[64];
    auto* p = buf;
    p = detail::pack(p, payload.term);
    p = detail::pack(p, payload.candidate_id);
    p = detail::pack(p, payload.last_log_idx);
    p = detail::pack(p, payload.last_log_term);
    const uint32_t req_len = static_cast<uint32_t>(p - buf);

    std::byte reply[INLINE_PAYLOAD_BYTES];
    uint32_t n = client_request(peer_ip, SERVER_PORT, buf, req_len,
                                reply, sizeof(reply));

    int  term         = 0;
    bool vote_granted = false;
    if (n >= sizeof(int) + sizeof(bool)) {
        const std::byte* q = reply;
        q = detail::unpack(q, &term);
        q = detail::unpack(q, &vote_granted);
    }
    return {term, vote_granted};
}

// returns current term #, for leader to update
inline int Node::send_install_snapshot_rpc(std::string_view peer_ip,
                                           const InstallSnapshotPayload& payload) {
    std::byte buf[64];
    auto* p = buf;
    p = detail::pack(p, payload.term);
    p = detail::pack(p, payload.leader_id);
    p = detail::pack(p, payload.last_included_idx);
    p = detail::pack(p, payload.last_included_term);
    p = detail::pack(p, payload.offset);
    p = detail::pack(p, payload.done);
    const uint32_t req_len = static_cast<uint32_t>(p - buf);

    std::byte reply[INLINE_PAYLOAD_BYTES];
    uint32_t n = client_request(peer_ip, SERVER_PORT, buf, req_len,
                                reply, sizeof(reply));

    int term = 0;
    if (n >= sizeof(int)) {
        detail::unpack(reply, &term);
    }
    return term;
}