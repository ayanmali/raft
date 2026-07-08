#pragma once
#include "payloads.hpp"
#include "../conns.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netinet/in.h>
#include <span>
#include <expected>

#if __BIG_ENDIAN__
# define htonll(x) (x)
# define ntohll(x) (x)
#else
# define htonll(x) (((uint64_t)htonl((x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
# define ntohll(x) (((uint64_t)ntohl((x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
struct ByteReader;

using ReqParserFunc = std::expected<RpcMessage, const char*>(*)(ByteReader&, FD);
using ReplyParserFunc = std::expected<RpcMessage, const char*>(*)(std::byte*);

std::expected<RpcMessage, const char*> parse_rbuf(std::byte* rbuf, uint32_t total_length);
std::expected<RpcMessage, const char*> parse_rbuf(ClientConn* c, uint32_t msg_len, size_t end, size_t parsed);

struct ByteReader {
    public:
    explicit ByteReader(std::span<const std::byte> bytes)
        : ptr(bytes.data()), end(bytes.data() + bytes.size_bytes()) {};


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

    bool read(std::byte* out, size_t size) {
        if (remaining() < size) return false;
        std::memcpy(out, ptr, size);
        ptr += size;
        return true;
    }

    bool read(uint8_t* out, size_t size) {
        if (remaining() < size) return false;
        std::memcpy(out, ptr, size);
        ptr += size;
        return true;
    }

    bool read (LogEntry* out, size_t n) {
        if (remaining() < n * sizeof(LogEntry)) return false;
        std::memcpy(out, ptr, n * sizeof(LogEntry));
        ptr += n * sizeof(LogEntry);

        for (size_t i = 0; i < n; ++i) {
            out[i].term = ntohl(out[i].term);
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

struct BufByteWriter {
    public:
    BufByteWriter(std::byte* buf_) : buf{buf_} {}
    void serialize(AppendEntriesReqPayload& payload);
    void serialize(const RequestVoteReqPayload& payload);
    void serialize(const InstallSnapshotReqPayload& payload);
    void serialize(const AppendEntriesRespPayload& payload);
    void serialize(const RequestVoteRespPayload& payload);
    void serialize(const InstallSnapshotRespPayload& payload);
    private:
    std::byte* buf;
};
