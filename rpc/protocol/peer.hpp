#include "./utils.hpp"
#include "../conns.hpp"
#include "payloads.hpp"

/* Inbound */

inline std::expected<RpcReply, const char*> parse_ae_reply(ByteReader& byte_reader) {
    AppendEntriesRespPayload response;

    if (!byte_reader.read(response.term)) return std::unexpected("failed to parse AppendEntries response term field");
    if (!byte_reader.read(response.success)) return std::unexpected("failed to parse AppendEntries response success field");
    return response;
}

inline std::expected<RpcReply, const char*> parse_rv_reply(ByteReader& byte_reader) {
    RequestVoteRespPayload response;

    if (!byte_reader.read(response.term)) return std::unexpected("failed to parse RequestVote response term field");
    if (!byte_reader.read(response.vote_granted)) return std::unexpected("failed to parse RequestVote response success field");
    return response;
} 

inline std::expected<RpcReply, const char*> parse_is_reply(ByteReader& byte_reader) {
    InstallSnapshotRespPayload response;

    if (!byte_reader.read(response.term)) return std::unexpected("failed to parse InstallSnapshot response term field");
    return response;
}

constexpr std::array<ReplyParserFunc, 4> make_reply_parser_table() {
    std::array<ReplyParserFunc, 4> table{};
    table[1] = parse_ae_reply;
    table[2] = parse_rv_reply;
    table[3] = parse_is_reply;

    return table;
}

constexpr auto REPLY_PARSER_TABLE = make_reply_parser_table();

inline std::expected<RpcReply, const char*> parse_reply_buffer(InflightRPC& rpc) {
    if (rpc.reply.size() < sizeof(uint32_t)) return std::unexpected("not enough data to read"); // need to see message size first
    uint32_t message_size;
    std::memcpy(&message_size, rpc.reply.data(), sizeof(message_size));
    message_size = ntohl(message_size);

    ByteReader byte_reader(std::span<std::byte>(rpc.reply.begin(), rpc.reply.begin() + message_size));

    auto func = REPLY_PARSER_TABLE[static_cast<size_t>(rpc.kind)];
    if (!func) return std::unexpected("invalid RPC id");

    rpc.reply.erase(rpc.reply.begin(), rpc.reply.begin() + sizeof(message_size));
    return func(byte_reader);
}
