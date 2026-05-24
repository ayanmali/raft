#pragma once
#include "payloads.hpp"
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <variant>

static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
struct ByteReader;

using RpcMessage = std::variant<AppendEntriesReqPayload, RequestVoteReqPayload, InstallSnapshotReqPayload, ArmTimerPayload, DisarmTimerPayload>; // nanoseconds used for arm timer messages
using RpcReply = std::variant<AppendEntriesRespPayload, RequestVoteRespPayload, InstallSnapshotRespPayload>;

using ParserFunc = std::expected<RpcMessage, const char*>(*)(ByteReader&);
using HandlerFunc = std::expected<RpcReply, const char*>(*)(const RpcMessage& message);
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
        out = ntohs(out);
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

    bool read(size_t& out) {
        if (remaining() < sizeof(out)) return false;

        std::memcpy(&out, ptr, sizeof(out));
        out = ntohll(out);
        ptr += sizeof(out);

        return true;
    }

    bool read(std::vector<std::byte>& out) {
        uint32_t size;
        if (!read(size)) return false;

        if (remaining() < size * sizeof(std::byte)) return false;

        out.resize(size);

        std::memcpy(out.data(), ptr, size * sizeof(std::byte));
        ptr += size * sizeof(std::byte);
        return true;
    }
    
    private:
    const std::byte* ptr;
    const std::byte* end;

    size_t remaining() const {
        return static_cast<size_t>(end - ptr);
    }

};

struct ClientReplyVisitor;