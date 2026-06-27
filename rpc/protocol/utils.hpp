#pragma once
#include "payloads.hpp"
#include "../conns.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <span>
#include <utility>
#include <expected>

#define ntohll(x) (((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#define htonll(x) (((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))

static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
struct ByteReader;

// Widen any of the narrower variants (RpcRequest/RpcReply) into RpcMessage.
// Each alternative of the source variant is also an alternative of RpcMessage,
// so a single std::visit handles the conversion uniformly.
// template <typename... Ts>
// inline RpcMessage to_rpc_message(const std::variant<Ts...>& v) {
//     return std::visit([](auto&& payload) -> RpcMessage { return payload; }, v);
// }

// template <typename... Ts>
// inline RpcMessage to_rpc_message(std::variant<Ts...>&& v) {
//     return std::visit([](auto&& payload) -> RpcMessage {
//         return std::move(payload);
//     }, std::move(v));
// }

// Accept a bare payload type (e.g. DropPeerMsg{}) that is itself an RpcMessage
// alternative, without requiring callers to wrap it in a variant first.
// template <typename T,
//           typename = std::enable_if_t<
//               std::is_constructible_v<RpcMessage, T&&>>>
// inline RpcMessage to_rpc_message(T&& payload) {
//     return RpcMessage(std::forward<T>(payload));
// }

using ReqParserFunc = std::expected<RpcMessage, const char*>(*)(ByteReader&, FD);
using ReplyParserFunc = std::expected<RpcMessage, const char*>(*)(ByteReader&);

std::expected<RpcMessage, const char*> parse_rbuf(InflightRPC& rpc);
std::expected<RpcMessage, const char*> parse_rbuf(ClientConn* c);

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
    void serialize(const AppendEntriesReqPayload& payload);
    void serialize(const RequestVoteReqPayload& payload);
    void serialize(const InstallSnapshotReqPayload& payload);
    void serialize(const AppendEntriesRespPayload& payload);
    void serialize(const RequestVoteRespPayload& payload);
    void serialize(const InstallSnapshotRespPayload& payload);
    private:
    std::vector<std::byte>& buf;
};
