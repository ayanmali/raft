#include "./utils.hpp"
#include "payloads.hpp"
#include "../../errors.hpp"
#include <netinet/in.h>

/* Inbound */

std::expected<RpcMessage, const char*> parse_ae_reply(std::byte* rbuf) {
    AppendEntriesRespPayload response;

    size_t ptr = 0;

    std::memcpy(&response.entries_len, rbuf, sizeof(response.entries_len));
    ptr += sizeof(response.entries_len);
    response.entries_len = ntohll(response.entries_len);

    std::memcpy(&response.server_id, rbuf + ptr, sizeof(response.server_id));
    ptr += sizeof(response.server_id);
    response.server_id = ntohl(response.server_id);

    std::memcpy(&response.term, rbuf + ptr, sizeof(response.term));
    ptr += sizeof(response.term);
    response.term = ntohl(response.term);

    std::memcpy(&response.success, rbuf + ptr, sizeof(response.success));
    ptr += sizeof(response.success);

    return response;
}

std::expected<RpcMessage, const char*> parse_rv_reply(std::byte* rbuf) {
    RequestVoteRespPayload response;

    size_t ptr = 0;

    std::memcpy(&response.server_id, rbuf, sizeof(response.server_id));
    ptr += sizeof(response.server_id);
    response.server_id = ntohl(response.server_id);

    std::memcpy(&response.term, rbuf + ptr, sizeof(response.term));
    ptr += sizeof(response.term);
    response.term = ntohl(response.term);

    std::memcpy(&response.vote_granted, rbuf + ptr, sizeof(response.vote_granted));
    ptr += sizeof(response.vote_granted);

    return response;
}

std::expected<RpcMessage, const char*> parse_is_reply(std::byte* rbuf) {
    InstallSnapshotRespPayload response;

    size_t ptr = 0;

    std::memcpy(&response.server_id, rbuf, sizeof(response.server_id));
    ptr += sizeof(response.server_id);
    response.server_id = ntohl(response.server_id);

    std::memcpy(&response.term, rbuf + ptr, sizeof(response.term));
    ptr += sizeof(response.term);
    response.term = ntohl(response.term);

    return response;
}

constexpr std::array<ReplyParserFunc, IS_RPC_ID + 1> make_reply_parser_table() {
    std::array<ReplyParserFunc, IS_RPC_ID + 1> table{};
    table[AE_RPC_ID] = parse_ae_reply;
    table[RV_RPC_ID] = parse_rv_reply;
    table[IS_RPC_ID] = parse_is_reply;

    return table;
}

constexpr auto REPLY_PARSER_TABLE = make_reply_parser_table();

std::expected<RpcMessage, const char*> parse_rbuf(std::byte* rbuf, uint32_t total_length) {
    uint8_t kind_byte;
    std::memcpy(&kind_byte, rbuf, sizeof(kind_byte));

    auto func = REPLY_PARSER_TABLE[kind_byte];
    if (!func)
        return Unexpected("invalid RPC kind");

    return func(rbuf + sizeof(kind_byte));
}

/* Outbound */

void BufByteWriter::serialize(AppendEntriesReqPayload& payload) {
    for (size_t i = 0; i < payload.entries_len; ++i) {
        payload.entries[i].term = htonl(payload.entries[i].term);
    }
    auto msg_size           =  htonl(payload.size() + sizeof(uint8_t));
    auto net_id             =  AE_RPC_ID;
    auto net_entries_len    =  htonll(payload.entries_len);
    auto net_term           =  htonl(payload.term);
    auto net_leader_id      =  htonl(payload.leader_id);
    auto net_prev_log_idx   =  htonl(payload.prev_log_idx);
    auto net_prev_log_term  =  htonl(payload.prev_log_term);
    auto net_leader_commit  =  htonl(payload.leader_commit);

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(buf + ptr, &net_entries_len, sizeof(net_entries_len));
    ptr += sizeof(net_entries_len);

    std::memcpy(buf + ptr, payload.entries, sizeof(LogEntry) * payload.entries_len);
    ptr += sizeof(LogEntry) * payload.entries_len;

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(buf + ptr, &net_leader_id, sizeof(net_leader_id));
    ptr += sizeof(net_leader_id);

    std::memcpy(buf + ptr, &net_prev_log_idx, sizeof(net_prev_log_idx));
    ptr += sizeof(net_prev_log_idx);

    std::memcpy(buf + ptr, &net_prev_log_term, sizeof(net_prev_log_term));
    ptr += sizeof(net_prev_log_term);

    std::memcpy(buf + ptr, &net_leader_commit, sizeof(net_leader_commit));
};

void BufByteWriter::serialize(const RequestVoteReqPayload& payload) {
    auto msg_size          = htonl(payload.size() + sizeof(uint8_t));
    auto net_id            = RV_RPC_ID;
    auto net_term          = htonl(payload.term);
    auto net_candidate_id  = htonl(payload.candidate_id);
    auto net_last_log_idx  = htonl(payload.last_log_idx);
    auto net_last_log_term = htonl(payload.last_log_term);

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(buf + ptr, &net_candidate_id, sizeof(net_candidate_id));
    ptr += sizeof(net_candidate_id);

    std::memcpy(buf + ptr, &net_last_log_idx, sizeof(net_last_log_idx));
    ptr += sizeof(net_last_log_idx);

    std::memcpy(buf + ptr, &net_last_log_term, sizeof(net_last_log_term));
};

void BufByteWriter::serialize(const InstallSnapshotReqPayload& payload) {
    auto msg_size               = htonl(payload.size() + sizeof(uint8_t));
    auto net_id                 = IS_RPC_ID;
    auto net_last_included_idx  = htonl(payload.last_included_idx);
    auto net_last_included_term = htonl(payload.last_included_term);
    auto net_offset             = htonll(payload.offset);
    auto net_term               = htonl(payload.term);
    auto net_leader_id          = htonl(payload.leader_id);
    auto net_done               = payload.done;

    size_t ptr = 0;

    std::memcpy(buf, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(buf + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(buf + ptr, payload.partial_state, sizeof(payload.partial_state));
    ptr += sizeof(payload.partial_state);

    std::memcpy(buf + ptr, &net_last_included_idx, sizeof(net_last_included_idx));
    ptr += sizeof(net_last_included_idx);

    std::memcpy(buf + ptr, &net_last_included_term, sizeof(net_last_included_term));
    ptr += sizeof(net_last_included_term);

    std::memcpy(buf + ptr, &net_offset, sizeof(net_offset));
    ptr += sizeof(net_offset);

    std::memcpy(buf + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(buf + ptr, &net_leader_id, sizeof(net_leader_id));
    ptr += sizeof(net_leader_id);

    std::memcpy(buf + ptr, &net_done, sizeof(net_done));
};
