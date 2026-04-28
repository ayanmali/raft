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
namespace detail {
template <class T>
inline std::byte* pack(std::byte* p, const T& v) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::memcpy(p, &v, sizeof(T));
    return p + sizeof(T);
}
template <class T>
inline const std::byte* unpack(const std::byte* p, T* v) {
    static_assert(std::is_trivially_copyable_v<T>);
    std::memcpy(v, p, sizeof(T));
    return p + sizeof(T);
}
} // namespace detail

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