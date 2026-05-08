#pragma once
#include "./conns.hpp"
#include "./payloads.hpp"
#include <arpa/inet.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <utility>
#include <vector>

// =============================================================================
// Buffer-backed (non-blocking) ser/de.
//
// The fd-backed helpers above call read_full/write_full which block until the
// kernel hands over the requested number of bytes. That's incompatible with an
// epoll loop that recv()s into a per-conn rbuf and needs to make progress
// without ever blocking.
//
// The functions below operate on caller-managed byte buffers:
//   - try_parse_*_req  : returns a payload iff the buffer contains a complete
//                        frame; sets *consumed to the number of bytes the
//                        caller should pop from the front of its rbuf.
//   - try_parse_*_resp : same, for replies (no id tag; caller knows the kind).
//   - serialize_*_req  : appends the wire-format bytes for a request to a
//                        std::vector<std::byte> wbuf.
//   - serialize_*_resp : same, for replies.
// =============================================================================


static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
static constexpr uint8_t AE_RPC_ID = 1;
static constexpr uint8_t RV_RPC_ID = 2;
static constexpr uint8_t IS_RPC_ID = 3;

namespace detail {

    inline uint32_t read_u32_be(const std::byte* p) noexcept {
        uint32_t net;
        std::memcpy(&net, p, sizeof(net));
        return ntohl(net);
    }
    
    inline uint8_t read_u8(const std::byte* p) noexcept {
        return static_cast<uint8_t>(*p);
    }
    
    inline void append_u32_be(std::vector<std::byte>& buf, uint32_t v) {
        uint32_t net = htonl(v);
        const auto* b = reinterpret_cast<const std::byte*>(&net);
        buf.insert(buf.end(), b, b + sizeof(net));
    }
    
    inline void append_u8(std::vector<std::byte>& buf, uint8_t v) {
        buf.push_back(static_cast<std::byte>(v));
    }
    
    inline void append_bytes(std::vector<std::byte>& buf, const std::byte* p, size_t n) {
        buf.insert(buf.end(), p, p + n);
    }
    
    } // namespace detail
    
    // ---- Request parsers -------------------------------------------------------
    
    inline std::optional<AppendEntriesReqPayload>
    try_parse_ae_req(const std::byte* buf, size_t avail, size_t* consumed) {
        // Layout: id(1) | term(4) | leader_id(4) | prev_log_idx(4) |
        //         prev_log_term(4) | leader_commit(4) | entries_len(4) | entries...
        constexpr size_t HDR = 1 + 4 * 6; // header + each member
        if (avail < HDR) return std::nullopt;
    
        uint32_t entries_len = detail::read_u32_be(buf + 1 + 4 * 5);
        if (entries_len > MAX_VECTOR_SIZE_SANITY) {
            throw std::runtime_error("AppendEntries entries vector exceeds sanity limit");
        }
        if (avail < HDR + entries_len) return std::nullopt;
    
        uint32_t term          = detail::read_u32_be(buf + 1 + 0);
        uint32_t leader_id     = detail::read_u32_be(buf + 1 + 4);
        uint32_t prev_log_idx  = detail::read_u32_be(buf + 1 + 8);
        uint32_t prev_log_term = detail::read_u32_be(buf + 1 + 12);
        uint32_t leader_commit = detail::read_u32_be(buf + 1 + 16);
    
        std::vector<std::byte> entries(buf + HDR, buf + HDR + entries_len);
        *consumed = HDR + entries_len;
        return AppendEntriesReqPayload(entries, term, leader_id,
                                       prev_log_idx, prev_log_term, leader_commit);
    }
    
    inline std::optional<RequestVoteReqPayload>
    try_parse_rv_req(const std::byte* buf, size_t avail, size_t* consumed) {
        constexpr size_t SZ = 1 + 4 * 4; // header + each member
        if (avail < SZ) return std::nullopt;
    
        uint32_t term          = detail::read_u32_be(buf + 1 + 0);
        uint32_t candidate_id  = detail::read_u32_be(buf + 1 + 4);
        uint32_t last_log_idx  = detail::read_u32_be(buf + 1 + 8);
        uint32_t last_log_term = detail::read_u32_be(buf + 1 + 12);
    
        *consumed = SZ;
        return RequestVoteReqPayload(term, candidate_id, last_log_idx, last_log_term);
    }
    
    inline std::optional<InstallSnapshotReqPayload>
    try_parse_is_req(const std::byte* buf, size_t avail, size_t* consumed) {
        // Layout: id(1) | term(4) | leader_id(4) | last_included_idx(4) |
        //         last_included_term(4) | offset(4) | done(1) | snapshot_len(4) | snapshot...
        constexpr size_t HDR = 1 + 4 * 5 + 1 + 4; // header + each member
        if (avail < HDR) return std::nullopt;
    
        uint32_t snapshot_len = detail::read_u32_be(buf + 1 + 4 * 5 + 1);
        if (snapshot_len > MAX_VECTOR_SIZE_SANITY) {
            throw std::runtime_error("InstallSnapshot snapshot vector exceeds sanity limit");
        }
        if (avail < HDR + snapshot_len) return std::nullopt;
    
        uint32_t term               = detail::read_u32_be(buf + 1 + 0);
        uint32_t leader_id          = detail::read_u32_be(buf + 1 + 4);
        uint32_t last_included_idx  = detail::read_u32_be(buf + 1 + 8);
        uint32_t last_included_term = detail::read_u32_be(buf + 1 + 12);
        uint32_t offset             = detail::read_u32_be(buf + 1 + 16);
        uint8_t  done               = detail::read_u8(buf + 1 + 4 * 5);
    
        std::vector<std::byte> snapshot(buf + HDR, buf + HDR + snapshot_len);
        *consumed = HDR + snapshot_len;
        return InstallSnapshotReqPayload(snapshot, term, leader_id,
                                         last_included_idx, last_included_term,
                                         offset, done);
    }
    
    // Peeks the id byte and dispatches. Returns true iff a full frame was
    // parsed; on success `*consumed` is set and `v(payload)` is invoked
    // exactly once with the strongly-typed payload. Throws on unknown id.
    template <class Visitor>
    inline bool try_parse_req(const std::byte* buf, size_t avail,
                              size_t* consumed, Visitor&& v) {
        if (avail < 1) return false;
        uint8_t id = detail::read_u8(buf);
        switch (id) {
            case AE_RPC_ID: {
                auto p = try_parse_ae_req(buf, avail, consumed);
                if (!p) return false;
                std::forward<Visitor>(v)(std::move(*p));
                return true;
            }
            case RV_RPC_ID: {
                auto p = try_parse_rv_req(buf, avail, consumed);
                if (!p) return false;
                std::forward<Visitor>(v)(std::move(*p));
                return true;
            }
            case IS_RPC_ID: {
                auto p = try_parse_is_req(buf, avail, consumed);
                if (!p) return false;
                std::forward<Visitor>(v)(std::move(*p));
                return true;
            }
            default:
                throw std::runtime_error("unknown RPC id");
        }
    }
    
    // ---- Reply parsers (no id byte; caller dispatches by RpcKind) --------------
    
    inline std::optional<AppendEntriesRespPayload>
    try_parse_ae_resp(const std::byte* buf, size_t avail, size_t* consumed) {
        constexpr size_t SZ = 4 + 1;
        if (avail < SZ) return std::nullopt;
        uint32_t term = detail::read_u32_be(buf + 0);
        uint8_t  ok   = detail::read_u8(buf + 4);
        *consumed = SZ;
        return AppendEntriesRespPayload{ term, ok };
    }
    
    inline std::optional<RequestVoteRespPayload>
    try_parse_rv_resp(const std::byte* buf, size_t avail, size_t* consumed) {
        constexpr size_t SZ = 4 + 1;
        if (avail < SZ) return std::nullopt;
        uint32_t term = detail::read_u32_be(buf + 0);
        uint8_t  vg   = detail::read_u8(buf + 4);
        *consumed = SZ;
        return RequestVoteRespPayload{ term, vg };
    }
    
    inline std::optional<InstallSnapshotRespPayload>
    try_parse_is_resp(const std::byte* buf, size_t avail, size_t* consumed) {
        constexpr size_t SZ = 4;
        if (avail < SZ) return std::nullopt;
        uint32_t term = detail::read_u32_be(buf + 0);
        *consumed = SZ;
        return InstallSnapshotRespPayload{ term };
    }
    
    // ---- Serializers (append wire bytes to a std::vector<std::byte>) -----------
    
    inline void serialize_ae_req(const AppendEntriesReqPayload& p,
                                 std::vector<std::byte>& out) {
        detail::append_u8(out,      AE_RPC_ID);
        detail::append_u32_be(out,  p.term);
        detail::append_u32_be(out,  p.leader_id);
        detail::append_u32_be(out,  p.prev_log_idx);
        detail::append_u32_be(out,  p.prev_log_term);
        detail::append_u32_be(out,  p.leader_commit);
        detail::append_u32_be(out,  static_cast<uint32_t>(p.entries.size()));
        detail::append_bytes(out,   p.entries.data(), p.entries.size());
    }
    
    inline void serialize_rv_req(const RequestVoteReqPayload& p,
                                 std::vector<std::byte>& out) {
        detail::append_u8(out,      RV_RPC_ID);
        detail::append_u32_be(out,  p.term);
        detail::append_u32_be(out,  p.candidate_id);
        detail::append_u32_be(out,  p.last_log_idx);
        detail::append_u32_be(out,  p.last_log_term);
    }
    
    inline void serialize_is_req(const InstallSnapshotReqPayload& p,
                                 std::vector<std::byte>& out) {
        detail::append_u8(out,      IS_RPC_ID);
        detail::append_u32_be(out,  p.term);
        detail::append_u32_be(out,  p.leader_id);
        detail::append_u32_be(out,  p.last_included_idx);
        detail::append_u32_be(out,  p.last_included_term);
        detail::append_u32_be(out,  p.offset);
        detail::append_u8(out,      p.done);
        detail::append_u32_be(out,  static_cast<uint32_t>(p.snapshot.size()));
        detail::append_bytes(out,   p.snapshot.data(), p.snapshot.size());
    }
    
    inline void serialize_ae_resp(const AppendEntriesRespPayload& p,
                                  std::vector<std::byte>& out) {
        detail::append_u32_be(out, p.term);
        detail::append_u8(out,     p.success);
    }

    inline void serialize_rv_resp(const RequestVoteRespPayload& p,
                                  std::vector<std::byte>& out) {
        detail::append_u32_be(out, p.term);
        detail::append_u8(out,     p.vote_granted);
    }

    inline void serialize_is_resp(const InstallSnapshotRespPayload& p,
                                  std::vector<std::byte>& out) {
        detail::append_u32_be(out, p.term);
    }

// =============================================================================
// Dispatch tables.
//
// The visitor + nested switch pattern (try_parse_req(buf, avail, *consumed,
// Visitor) above) requires the call site to mirror the dispatcher's branching
// shape with a compile-time `if constexpr` chain. Visually that's redundant.
//
// The two tables below collapse the id-to-action mapping into a single
// place per direction:
//   - kRpcEntries[id]                         -> parse_and_handle_*
//   - kReplyEntries[static_cast<size_t>(kind)] -> parse_and_invoke_*_reply
//
// At runtime each call site becomes "bounds check -> indexed indirect call".
// The indirect call uses the BTB, which is well-predicted for stable id
// distributions, so this is no worse than the switch it replaces -- and the
// visitor lambda + `if constexpr` chain at the call site go away.
// =============================================================================

// Synchronous request handlers the loop calls when a complete inbound frame
// is parsed. Node populates these with member-function shims that acquire
// state_mu_ and produce a response payload.
struct RpcHandlers {
    std::function<AppendEntriesRespPayload(const AppendEntriesReqPayload&)>     on_ae_req;
    std::function<RequestVoteRespPayload(const RequestVoteReqPayload&)>         on_rv_req;
    std::function<InstallSnapshotRespPayload(const InstallSnapshotReqPayload&)> on_is_req;
};

// ---- Request side: parse + handle + serialize-resp ------------------------

using RpcEntry = size_t (*)(const std::byte* buf, size_t avail,
                            std::vector<std::byte>& wbuf,
                            const RpcHandlers& h);

inline size_t parse_and_handle_ae(const std::byte* buf, size_t avail,
                                  std::vector<std::byte>& wbuf,
                                  const RpcHandlers& h) {
    size_t consumed = 0;
    auto p = try_parse_ae_req(buf, avail, &consumed);
    if (!p) return 0;
    serialize_ae_resp(h.on_ae_req(*p), wbuf);
    return consumed;
}

inline size_t parse_and_handle_rv(const std::byte* buf, size_t avail,
                                  std::vector<std::byte>& wbuf,
                                  const RpcHandlers& h) {
    size_t consumed = 0;
    auto p = try_parse_rv_req(buf, avail, &consumed);
    if (!p) return 0;
    serialize_rv_resp(h.on_rv_req(*p), wbuf);
    return consumed;
}

inline size_t parse_and_handle_is(const std::byte* buf, size_t avail,
                                  std::vector<std::byte>& wbuf,
                                  const RpcHandlers& h) {
    size_t consumed = 0;
    auto p = try_parse_is_req(buf, avail, &consumed);
    if (!p) return 0;
    serialize_is_resp(h.on_is_req(*p), wbuf);
    return consumed;
}

// Indexed by wire id. Slot 0 unused (sentinel = nullptr); valid ids are
// AE_RPC_ID (1), RV_RPC_ID (2), IS_RPC_ID (3).
inline constexpr RpcEntry kRpcEntries[] = {
    nullptr,                 // 0
    &parse_and_handle_ae,    // AE_RPC_ID = 1
    &parse_and_handle_rv,    // RV_RPC_ID = 2
    &parse_and_handle_is,    // IS_RPC_ID = 3
};

// Single entry point used by EventLoop::DispatchOneRequest. Returns true iff
// a full frame was parsed and the response was serialized into wbuf; on
// success *consumed is set to the number of bytes the caller should pop
// from the front of its rbuf. Throws on unknown id.
inline bool try_parse_and_handle_req(const std::byte* buf, size_t avail,
                                     size_t* consumed,
                                     std::vector<std::byte>& wbuf,
                                     const RpcHandlers& h) {
    if (avail < 1) { *consumed = 0; return false; }
    uint8_t id = detail::read_u8(buf);
    if (id >= std::size(kRpcEntries) || !kRpcEntries[id]) {
        throw std::runtime_error("unknown RPC id");
    }
    *consumed = kRpcEntries[id](buf, avail, wbuf, h); // sending reply
    return *consumed != 0;
}

// ---- Reply side: parse + invoke matching callback -------------------------

using ReplyEntry = size_t (*)(const std::byte* buf, size_t avail,
                              const PendingReply& pr);

inline size_t parse_and_invoke_ae_reply(const std::byte* buf, size_t avail,
                                        const PendingReply& pr) {
    size_t consumed = 0;
    auto r = try_parse_ae_resp(buf, avail, &consumed);
    if (!r) return 0;
    if (pr.on_ae) pr.on_ae(*r);
    return consumed;
}

inline size_t parse_and_invoke_rv_reply(const std::byte* buf, size_t avail,
                                        const PendingReply& pr) {
    size_t consumed = 0;
    auto r = try_parse_rv_resp(buf, avail, &consumed);
    if (!r) return 0;
    if (pr.on_rv) pr.on_rv(*r);
    return consumed;
}

inline size_t parse_and_invoke_is_reply(const std::byte* buf, size_t avail,
                                        const PendingReply& pr) {
    size_t consumed = 0;
    auto r = try_parse_is_resp(buf, avail, &consumed);
    if (!r) return 0;
    if (pr.on_is) pr.on_is(*r);
    return consumed;
}

// Indexed by static_cast<size_t>(RpcKind). RpcKind is dense and 0-based, so
// no bounds check is needed at the call site.
inline constexpr ReplyEntry kReplyEntries[] = {
    &parse_and_invoke_ae_reply,  // RpcKind::AppendEntries   = 0
    &parse_and_invoke_rv_reply,  // RpcKind::RequestVote     = 1
    &parse_and_invoke_is_reply,  // RpcKind::InstallSnapshot = 2
};