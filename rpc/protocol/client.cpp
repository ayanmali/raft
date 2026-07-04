#include "../conns.hpp"
#include "./utils.hpp"
#include "../../errors.hpp"
#include "payloads.hpp"
#include <expected>

/* Inbound */

std::expected<RpcMessage, const char*> parse_ae_req(ByteReader& byte_reader, FD fd) {
    AppendEntriesReqPayload message;

    message.fd = fd;
    if (!byte_reader.read(message.entries)) return Unexpected("failed to parse AppendEntries entries field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse AppendEntries term field");
    if (!byte_reader.read(message.leader_id)) return Unexpected("failed to parse AppendEntries leader_id field");
    if (!byte_reader.read(message.prev_log_idx)) return Unexpected("failed to parse AppendEntries prev_log_idx field");
    if (!byte_reader.read(message.prev_log_term)) return Unexpected("failed to parse AppendEntries prev_log_term field");
    if (!byte_reader.read(message.leader_commit)) return Unexpected("failed to parse AppendEntries leader_commit field");

    return message;
}

std::expected<RpcMessage, const char*> parse_rv_req(ByteReader& byte_reader, FD fd) {
    RequestVoteReqPayload message;

    message.fd = fd;
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse RequestVote term field");
    if (!byte_reader.read(message.candidate_id)) return Unexpected("failed to parse RequestVote candidate_id field");
    if (!byte_reader.read(message.last_log_idx)) return Unexpected("failed to parse RequestVote last_log_idx field");
    if (!byte_reader.read(message.last_log_term)) return Unexpected("failed to parse RequestVote last_log_term field");

    return message;
}

std::expected<RpcMessage, const char*> parse_is_req(ByteReader& byte_reader, FD fd) {
    InstallSnapshotReqPayload message;

    message.fd = fd;
    if (!byte_reader.read(message.snapshot.state, sizeof(message.snapshot.state))) return Unexpected("failed to parse InstallSnapshot snapshot field");
    if (!byte_reader.read(message.snapshot.last_included_idx)) return Unexpected("failed to parse InstallSnapshot last_included_idx field");
    if (!byte_reader.read(message.snapshot.last_included_term)) return Unexpected("failed to parse InstallSnapshot last_included_term field");
    if (!byte_reader.read(message.offset)) return Unexpected("failed to parse InstallSnapshot offset field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse InstallSnapshot term field");
    if (!byte_reader.read(message.leader_id)) return Unexpected("failed to parse InstallSnapshot leader_id field");
    if (!byte_reader.read(message.done)) return Unexpected("failed to parse InstallSnapshot done field");

    return message;
}

constexpr std::array<ReqParserFunc, IS_REPLY_ID - IS_RPC_ID> make_parser_table() {
    std::array<ReqParserFunc, IS_REPLY_ID - IS_RPC_ID> table{};
    table[AE_REPLY_ID - IS_RPC_ID - 1] = parse_ae_req;
    table[RV_REPLY_ID - IS_RPC_ID - 1] = parse_rv_req;
    table[IS_REPLY_ID - IS_RPC_ID - 1] = parse_is_req;

    return table;
}

constexpr auto PARSER_TABLE = make_parser_table();

std::expected<RpcMessage, const char*> parse_rbuf(ClientConn* c) {
    if (c->rbuf.size() < sizeof(uint32_t)) { return Unexpected("not enough data to read"); } // need to see message size first

    uint32_t message_size;
    std::memcpy(&message_size, c->rbuf.data(), sizeof(message_size));
    message_size = ntohl(message_size);

    ByteReader byte_reader(std::span<std::byte>(c->rbuf.begin() + sizeof(message_size), c->rbuf.begin() + sizeof(message_size) + message_size));
    uint8_t rpc_id;

    if (!byte_reader.read(rpc_id)) return Unexpected("failed to parse RPC id");

    auto func = PARSER_TABLE[rpc_id];
    if (!func) return Unexpected("invalid RPC id");

    c->rbuf.erase(c->rbuf.begin(), c->rbuf.begin() + sizeof(message_size) + message_size);
    return func(byte_reader, c->fd);
}

/* Outbound */

void BufByteWriter::serialize(const AppendEntriesRespPayload& payload) {
    auto msg_size        = htonl(payload.size() + sizeof(RpcKind));
    auto kind            = static_cast<uint8_t>(RpcKind::AppendEntries);
    auto net_entries_len = htonll(payload.entries_len);
    auto net_server_id   = htonl(payload.server_id);
    auto net_term        = htonl(payload.term);
    auto net_success     = payload.success;

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &kind, sizeof(kind));
    ptr += sizeof(kind);

    std::memcpy(buf + ptr, &net_entries_len, sizeof(net_entries_len));
    ptr += sizeof(net_entries_len);

    std::memcpy(buf + ptr, &net_server_id, sizeof(net_server_id));
    ptr += sizeof(net_server_id);

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(buf + ptr, &net_success, sizeof(net_success));
}

void BufByteWriter::serialize(const RequestVoteRespPayload& payload) {
    auto msg_size         = htonl(payload.size() + sizeof(RpcKind));
    auto kind             = static_cast<uint8_t>(RpcKind::RequestVote);
    auto net_server_id    = htonl(payload.server_id);
    auto net_term         = htonl(payload.term);
    auto net_vote_granted = payload.vote_granted;

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &kind, sizeof(kind));
    ptr += sizeof(kind);

    std::memcpy(buf + ptr, &net_server_id, sizeof(net_server_id));
    ptr += sizeof(net_server_id);

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(buf + ptr, &net_vote_granted, sizeof(net_vote_granted));
}

void BufByteWriter::serialize(const InstallSnapshotRespPayload& payload) {
    auto msg_size      = htonl(payload.size() + sizeof(RpcKind));
    auto kind          = static_cast<uint8_t>(RpcKind::InstallSnapshot);
    auto net_server_id = htonl(payload.server_id);
    auto net_term      = htonl(payload.term);

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &kind, sizeof(kind));
    ptr += sizeof(kind);

    std::memcpy(buf + ptr, &net_server_id, sizeof(net_server_id));
    ptr += sizeof(net_server_id);

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
}
