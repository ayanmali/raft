#include "../conns.hpp"
#include "./utils.hpp"
#include "../../errors.hpp"
#include "payloads.hpp"
#include <expected>

/* Inbound */

std::expected<RpcMessage, const char*> parse_ae_req(ByteReader& byte_reader, FD fd) {
    AppendEntriesReqPayload message;

    message.fd = fd;
    if (!byte_reader.read(message.entries_len)) return Unexpected("failed to parse AppendEntries entries_len field");
    if (!byte_reader.read(message.entries, message.entries_len)) return Unexpected("failed to parse AppendEntries entries field");
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
    if (!byte_reader.read(message.partial_state, sizeof(message.partial_state))) return Unexpected("failed to parse InstallSnapshot snapshot field");
    if (!byte_reader.read(message.cluster_raw_size)) return Unexpected("failed to parse InstallSnapshot cluster_raw_size field");
    if (!byte_reader.read(message.cluster, message.cluster_raw_size)) return Unexpected("failed to parse InstallSnapshot cluster field");
    if (!byte_reader.read(message.last_included_idx)) return Unexpected("failed to parse InstallSnapshot last_included_idx field");
    if (!byte_reader.read(message.last_included_term)) return Unexpected("failed to parse InstallSnapshot last_included_term field");
    if (!byte_reader.read(message.offset)) return Unexpected("failed to parse InstallSnapshot offset field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse InstallSnapshot term field");
    if (!byte_reader.read(message.leader_id)) return Unexpected("failed to parse InstallSnapshot leader_id field");
    if (!byte_reader.read(message.done)) return Unexpected("failed to parse InstallSnapshot done field");

    return message;
}

std::expected<RpcMessage, const char*> parse_fl_req(ByteReader& byte_reader, FD fd) {
    ForwardLeaderMsg message;

    message.fd = fd;
    if (!byte_reader.read(message.entries_len)) return Unexpected("failed to parse ForwardLeader entries_len field");
    if (!byte_reader.read(message.entries, message.entries_len)) return Unexpected("failed to parse ForwardLeader entries field");
    if (!byte_reader.read(message.sender_id)) return Unexpected("failed to parse ForwardLeader sender ID field");
    if (!byte_reader.read(message.term)) return Unexpected("failed to parse ForwardLeader term field");

    return message;
}

constexpr std::array<ReqParserFunc, 4> make_parser_table() {
    std::array<ReqParserFunc, 4> table{};
    table[0] = parse_ae_req;
    table[1] = parse_rv_req;
    table[2] = parse_is_req;
    table[3] = parse_fl_req;

    return table;
}

constexpr auto PARSER_TABLE = make_parser_table();

std::expected<RpcMessage, const char*> parse_rbuf(ClientConn* c, uint32_t message_size, size_t end, size_t parsed) {
    // if (sizeof(c->rbuf_) < sizeof(uint32_t)) { return Unexpected("not enough data to read"); } // need to see message size first
    // ByteReader byte_reader(std::span<std::byte>(c->rbuf_ + sizeof(message_size), c->rbuf_ + sizeof(message_size) + message_size));
    ByteReader byte_reader(std::span<std::byte>(c->rbuf + parsed + sizeof(message_size), end - sizeof(message_size) - parsed));
    uint8_t rpc_id;

    if (!byte_reader.read(rpc_id)) return Unexpected("failed to parse RPC id");

    auto func = PARSER_TABLE[rpc_id];
    if (!func) return Unexpected("invalid RPC id");

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
