#pragma once
#include "../conns.hpp"
#include "./utils.hpp"
#include "../../errors.hpp"
#include "payloads.hpp"
#include <expected>

/* Inbound */

inline std::expected<RpcRequest, const char*> parse_ae_req(ByteReader& byte_reader, IPAddress client_ip_addr) {
    AppendEntriesReqPayload message;

    if (!byte_reader.read(message.entries)) return Unexpected("failed to parse AppendEntries entries field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse AppendEntries term field");
    if (!byte_reader.read(message.leader_id)) return Unexpected("failed to parse AppendEntries leader_id field");
    if (!byte_reader.read(message.prev_log_idx)) return Unexpected("failed to parse AppendEntries prev_log_idx field");
    if (!byte_reader.read(message.prev_log_term)) return Unexpected("failed to parse AppendEntries prev_log_term field");
    if (!byte_reader.read(message.leader_commit)) return Unexpected("failed to parse AppendEntries leader_commit field");

    std::strcpy(message.client_ip_addr, client_ip_addr);
    return message;
}

inline std::expected<RpcRequest, const char*> parse_rv_req(ByteReader& byte_reader, IPAddress client_ip_addr) {
    RequestVoteReqPayload message;

    if (!byte_reader.read(message.term)) return Unexpected("failed to parse RequestVote term field");
    if (!byte_reader.read(message.candidate_id)) return Unexpected("failed to parse RequestVote candidate_id field");
    if (!byte_reader.read(message.last_log_idx)) return Unexpected("failed to parse RequestVote last_log_idx field");
    if (!byte_reader.read(message.last_log_term)) return Unexpected("failed to parse RequestVote last_log_term field");

    std::strcpy(message.client_ip_addr, client_ip_addr);
    return message;
}

inline std::expected<RpcRequest, const char*> parse_is_req(ByteReader& byte_reader, IPAddress client_ip_addr) {
    InstallSnapshotReqPayload message;

    if (!byte_reader.read(message.snapshot)) return Unexpected("failed to parse InstallSnapshot snapshot field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse InstallSnapshot term field");
    if (!byte_reader.read(message.leader_id)) return Unexpected("failed to parse InstallSnapshot leader_id field");
    if (!byte_reader.read(message.last_included_idx)) return Unexpected("failed to parse InstallSnapshot last_included_idx field");
    if (!byte_reader.read(message.last_included_term)) return Unexpected("failed to parse InstallSnapshot last_included_term field");
    if (!byte_reader.read(message.offset)) return Unexpected("failed to parse InstallSnapshot offset field");
    if (!byte_reader.read(message.done)) return Unexpected("failed to parse InstallSnapshot done field");

    std::strcpy(message.client_ip_addr, client_ip_addr);
    return message;
}

// TODO: verify this is correct
constexpr std::array<ParserFunc, 3> make_parser_table() {
    std::array<ParserFunc, 3> table{};
    table[0] = parse_ae_req;
    table[1] = parse_rv_req;
    table[2] = parse_is_req;

    return table;
}

constexpr auto PARSER_TABLE = make_parser_table();

inline std::expected<RpcRequest, const char*> parse_rbuf(ClientConn* c) {
    if (c->rbuf.size() < sizeof(uint32_t)) return Unexpected("not enough data to read"); // need to see message size first
    uint32_t message_size;
    std::memcpy(&message_size, c->rbuf.data(), sizeof(message_size));
    message_size = ntohl(message_size);

    ByteReader byte_reader(std::span<std::byte>(c->rbuf.begin(), c->rbuf.begin() + message_size));
    uint8_t rpc_id;

    if (!byte_reader.read(rpc_id)) return Unexpected("failed to parse RPC id");

    auto func = PARSER_TABLE[rpc_id];
    if (!func) return Unexpected("invalid RPC id");

    c->rbuf.erase(c->rbuf.begin(), c->rbuf.begin() + sizeof(message_size) + sizeof(rpc_id));
    return func(byte_reader, c->client_ip_addr);
}
