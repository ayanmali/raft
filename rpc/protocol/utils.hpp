#pragma once
#include "payloads.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <span>
#include <utility>
#include <variant>
#include <expected>

#define ntohll(x) (((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#define htonll(x) (((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))

static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
struct ByteReader;

// Widen any of the narrower variants (RpcRequest/RpcReply) into RpcMessage.
// Each alternative of the source variant is also an alternative of RpcMessage,
// so a single std::visit handles the conversion uniformly.
template <typename... Ts>
inline RpcMessage to_rpc_message(const std::variant<Ts...>& v) {
    return std::visit([](auto&& payload) -> RpcMessage { return payload; }, v);
}

template <typename... Ts>
inline RpcMessage to_rpc_message(std::variant<Ts...>&& v) {
    return std::visit([](auto&& payload) -> RpcMessage {
        return std::move(payload);
    }, std::move(v));
}

// Accept a bare payload type (e.g. DropPeerMsg{}) that is itself an RpcMessage
// alternative, without requiring callers to wrap it in a variant first.
template <typename T,
          typename = std::enable_if_t<
              std::is_constructible_v<RpcMessage, T&&>>>
inline RpcMessage to_rpc_message(T&& payload) {
    return RpcMessage(std::forward<T>(payload));
}

using ParserFunc = std::expected<RpcRequest, const char*>(*)(ByteReader&, IPAddress);
using HandlerFunc = std::expected<RpcReply, const char*>(*)(const RpcRequest& message);
using ReplyParserFunc = std::expected<RpcReply, const char*>(*)(ByteReader&);

struct ByteReader {
    public:
    explicit ByteReader(std::span<const std::byte> bytes)
        : ptr(bytes.data()), end(bytes.data() + bytes.size_bytes()) {};

    template <typename T>
    bool read(T& out) {
        if (remaining() < sizeof(T)) return false;

        std::memcpy(&out, ptr, sizeof(T));
        ptr += sizeof(T);

        return true;
    }

    bool read(uint8_t& out) {
        if (remaining() < sizeof(out)) return false;

        std::memcpy(&out, ptr, sizeof(out));
        ptr += sizeof(out);

        return true;
    }

    bool read(uint16_t& out) {
        if (remaining() < sizeof(out)) return false;

        std::memcpy(&out, ptr, sizeof(out));
        out = ntohs(out);
        ptr += sizeof(out);

        return true;
    }

    bool read(uint32_t& out) {
        if (remaining() < sizeof(out)) return false;

        std::memcpy(&out, ptr, sizeof(out));
        out = ntohl(out);
        ptr += sizeof(out);

        return true;
    }

    bool read(uint64_t& out) {
        if (remaining() < sizeof(out)) return false;

        std::memcpy(&out, ptr, sizeof(out));
        out = ntohll(out);
        ptr += sizeof(out);

        return true;
    }

    bool read(std::vector<std::byte>& out) {
        uint64_t size;
        if (!read(size)) return false;

        if (remaining() < size * sizeof(std::byte)) return false;

        out.resize(size);

        std::memcpy(out.data(), ptr, size * sizeof(std::byte));
        ptr += size * sizeof(std::byte);
        return true;
    }

    bool read(LogEntry& out) {
        if (!read(out.data)) return false;
        if (!read(out.term)) return false;
        return true;
    }

    bool read(std::vector<LogEntry>& out) {
        uint64_t size;
        if (!read(size)) return false;
        out.reserve(size);
        for (uint64_t i = 0; i < size; ++i) {
            LogEntry entry;
            if (!read(entry)) return false;
            out.push_back(std::move(entry));
        }
        return true;
    }

    private:
    const std::byte* ptr;
    const std::byte* end;

    size_t remaining() const {
        return static_cast<size_t>(end - ptr);
    }

};

struct ByteWriter {
    public:
    ByteWriter(std::vector<std::byte>& buf_) : buf{buf_} {}

    void serialize(const AppendEntriesReqPayload& payload) {
        auto msg_size           =  htonll(payload.size());
        auto net_id             =  AE_RPC_ID;
        auto net_entries_len    =  htonll(payload.entries.size());
        auto net_term           =  htonl(payload.term);
        auto net_leader_id      =  htonl(payload.leader_id);
        auto net_prev_log_idx   =  htonl(payload.prev_log_idx);
        auto net_prev_log_term  =  htonl(payload.prev_log_term);
        auto net_leader_commit  =  htonl(payload.leader_commit);

        buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_entries_len, sizeof(net_entries_len));
        ptr += sizeof(net_entries_len);

        for (const auto& entry : payload.entries) {
            uint64_t entry_data_len = htonll(entry.data.size());
            std::memcpy(buf.data() + ptr, &entry_data_len, sizeof(entry_data_len));
            ptr += sizeof(entry_data_len);

            std::memcpy(buf.data() + ptr, entry.data.data(), entry.data.size());
            ptr += entry.data.size();

            uint32_t entry_term = htonl(entry.term);
            std::memcpy(buf.data() + ptr, &entry_term, sizeof(entry_term));
            ptr += sizeof(entry_term);
        }

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_leader_id, sizeof(net_leader_id));
        ptr += sizeof(net_leader_id);

        std::memcpy(buf.data() + ptr, &net_prev_log_idx, sizeof(net_prev_log_idx));
        ptr += sizeof(net_prev_log_idx);

        std::memcpy(buf.data() + ptr, &net_prev_log_term, sizeof(net_prev_log_term));
        ptr += sizeof(net_prev_log_term);

        std::memcpy(buf.data() + ptr, &net_leader_commit, sizeof(net_leader_commit));

    };

    void serialize(const RequestVoteReqPayload& payload) {
        auto msg_size          = htonll(payload.size());
        auto net_id            = RV_RPC_ID;
        auto net_term          = htonl(payload.term);
        auto net_candidate_id  = htonl(payload.candidate_id);
        auto net_last_log_idx  = htonl(payload.last_log_idx);
        auto net_last_log_term = htonl(payload.last_log_term);

        buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_candidate_id, sizeof(net_candidate_id));
        ptr += sizeof(net_candidate_id);

        std::memcpy(buf.data() + ptr, &net_last_log_idx, sizeof(net_last_log_idx));
        ptr += sizeof(net_last_log_idx);

        std::memcpy(buf.data() + ptr, &net_last_log_term, sizeof(net_last_log_term));

    };

    void serialize(const InstallSnapshotReqPayload& payload) {
        auto msg_size               = htonll(payload.size());
        auto net_id                 = IS_RPC_ID;
        auto net_snapshot_len       = htonll(payload.snapshot.size());
        auto net_term               = htonl(payload.term);
        auto net_leader_id          = htonl(payload.leader_id);
        auto net_last_included_idx  = htonl(payload.last_included_idx);
        auto net_last_included_term = htonl(payload.last_included_term);
        auto net_offset             = htonl(payload.offset);
        auto net_done               = payload.done;

        buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_snapshot_len, sizeof(net_snapshot_len));
        ptr += sizeof(net_snapshot_len);

        std::memcpy(buf.data() + ptr, payload.snapshot.data(), payload.snapshot.size());
        ptr += payload.snapshot.size();

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_leader_id, sizeof(net_leader_id));
        ptr += sizeof(net_leader_id);

        std::memcpy(buf.data() + ptr, &net_last_included_idx, sizeof(net_last_included_idx));
        ptr += sizeof(net_last_included_idx);

        std::memcpy(buf.data() + ptr, &net_last_included_term, sizeof(net_last_included_term));
        ptr += sizeof(net_last_included_term);

        std::memcpy(buf.data() + ptr, &net_offset, sizeof(net_offset));
        ptr += sizeof(net_offset);

        std::memcpy(buf.data() + ptr, &net_done, sizeof(net_done));

    };

    void serialize(const AppendEntriesRespPayload& payload) {
        auto msg_size        = htonll(payload.size());
        //auto net_id = htons(AE_REPLY_ID);
        auto net_entries_len = htonll(payload.entries_len);
        auto net_term        = htonl(payload.term);
        auto net_success     = payload.success;

        // buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        buf.resize(payload.size() + sizeof(msg_size));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        // std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        // ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_entries_len, sizeof(net_entries_len));
        ptr += sizeof(net_entries_len);

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_success, sizeof(net_success));
    }

    void serialize(const RequestVoteRespPayload& payload) {
        auto msg_size         = htonll(payload.size());
        //auto net_id = htons(RV_REPLY_ID);
        auto net_term         = htonl(payload.term);
        auto net_vote_granted = payload.vote_granted;

        // buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        buf.resize(payload.size() + sizeof(msg_size));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        // std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        // ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_vote_granted, sizeof(net_vote_granted));
    }

    void serialize(const InstallSnapshotRespPayload& payload) {
        auto msg_size = htonll(payload.size());
        //auto net_id = htons(IS_REPLY_ID);
        auto net_term = htonl(payload.term);

        // buf.resize(payload.size() + sizeof(msg_size) + sizeof(net_id));
        buf.resize(payload.size() + sizeof(msg_size));
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        // std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        // ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
    }

    private:
    std::vector<std::byte>& buf;
};
