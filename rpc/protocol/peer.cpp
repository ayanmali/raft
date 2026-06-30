#include "./utils.hpp"
#include "payloads.hpp"
#include "../../errors.hpp"

/* Inbound */

std::expected<RpcMessage, const char*> parse_ae_reply(ByteReader& byte_reader) {
    AppendEntriesRespPayload response;

    if (!byte_reader.read(response.entries_len)) return Unexpected("failed to parse AppendEntries response entries_len field");
    if (!byte_reader.read(response.server_id)) return Unexpected("failed to parse AppendEntries server id field");
    if (!byte_reader.read(response.term)) return Unexpected("failed to parse AppendEntries response term field");
    if (!byte_reader.read(response.success)) return Unexpected("failed to parse AppendEntries response success field");
    return response;
}

std::expected<RpcMessage, const char*> parse_rv_reply(ByteReader& byte_reader) {
    RequestVoteRespPayload response;

    if (!byte_reader.read(response.server_id)) return Unexpected("failed to parse RequestVote server id field");
    if (!byte_reader.read(response.term)) return Unexpected("failed to parse RequestVote response term field");
    if (!byte_reader.read(response.vote_granted)) return Unexpected("failed to parse RequestVote response success field");
    return response;
}

std::expected<RpcMessage, const char*> parse_is_reply(ByteReader& byte_reader) {
    InstallSnapshotRespPayload response;

    if (!byte_reader.read(response.server_id)) return Unexpected("failed to parse InstallSnapshot server id field");
    if (!byte_reader.read(response.term)) return Unexpected("failed to parse InstallSnapshot response term field");
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

std::expected<RpcMessage, const char*> parse_rbuf(std::vector<std::byte>& rbuf) {
    if (rbuf.size() < sizeof(uint32_t))
        return Unexpected("not enough data to read");

    uint32_t total_length;
    std::memcpy(&total_length, rbuf.data(), sizeof(total_length));
    total_length = ntohl(total_length);

    if (rbuf.size() < sizeof(uint32_t) + total_length)
        return Unexpected("not enough data to read");

    ByteReader byte_reader(
        std::span<std::byte>(rbuf.begin() + sizeof(uint32_t),
                             rbuf.begin() + sizeof(uint32_t) + total_length));

    uint8_t kind_byte;
    if (!byte_reader.read(kind_byte))
        return Unexpected("failed to read RPC kind");

    auto func = REPLY_PARSER_TABLE[kind_byte];
    if (!func)
        return Unexpected("invalid RPC kind");

    rbuf.erase(rbuf.begin(), rbuf.begin() + sizeof(uint32_t) + total_length);
    return func(byte_reader);
}

/* Outbound */

void VecByteWriter::serialize(const AppendEntriesReqPayload& payload) {
    auto msg_size           =  htonl(payload.size() + sizeof(uint8_t));
    auto net_id             =  AE_RPC_ID;
    auto net_entries_len    =  htonll(payload.entries.size());
    auto net_term           =  htonl(payload.term);
    auto net_leader_id      =  htonl(payload.leader_id);
    auto net_prev_log_idx   =  htonl(payload.prev_log_idx);
    auto net_prev_log_term  =  htonl(payload.prev_log_term);
    auto net_leader_commit  =  htonl(payload.leader_commit);

    vec.resize(offset + payload.size() + sizeof(msg_size) + sizeof(net_id));
    size_t ptr = offset;

    std::memcpy(vec.data() + ptr, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(vec.data() + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(vec.data() + ptr, &net_entries_len, sizeof(net_entries_len));
    ptr += sizeof(net_entries_len);

    for (const auto& entry : payload.entries) {
        uint64_t entry_data_len = htonll(entry.data.size());
        std::memcpy(vec.data() + ptr, &entry_data_len, sizeof(entry_data_len));
        ptr += sizeof(entry_data_len);

        std::memcpy(vec.data() + ptr, entry.data.data(), entry.data.size());
        ptr += entry.data.size();

        uint32_t entry_term = htonl(entry.term);
        std::memcpy(vec.data() + ptr, &entry_term, sizeof(entry_term));
        ptr += sizeof(entry_term);
    }

    std::memcpy(vec.data() + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(vec.data() + ptr, &net_leader_id, sizeof(net_leader_id));
    ptr += sizeof(net_leader_id);

    std::memcpy(vec.data() + ptr, &net_prev_log_idx, sizeof(net_prev_log_idx));
    ptr += sizeof(net_prev_log_idx);

    std::memcpy(vec.data() + ptr, &net_prev_log_term, sizeof(net_prev_log_term));
    ptr += sizeof(net_prev_log_term);

    std::memcpy(vec.data() + ptr, &net_leader_commit, sizeof(net_leader_commit));
}

void VecByteWriter::serialize(const RequestVoteReqPayload& payload) {
    auto msg_size          = htonl(payload.size() + sizeof(uint8_t));
    auto net_id            = RV_RPC_ID;
    auto net_term          = htonl(payload.term);
    auto net_candidate_id  = htonl(payload.candidate_id);
    auto net_last_log_idx  = htonl(payload.last_log_idx);
    auto net_last_log_term = htonl(payload.last_log_term);

    vec.resize(offset + payload.size() + sizeof(msg_size) + sizeof(net_id));
    size_t ptr = offset;

    std::memcpy(vec.data() + ptr, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(vec.data() + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(vec.data() + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(vec.data() + ptr, &net_candidate_id, sizeof(net_candidate_id));
    ptr += sizeof(net_candidate_id);

    std::memcpy(vec.data() + ptr, &net_last_log_idx, sizeof(net_last_log_idx));
    ptr += sizeof(net_last_log_idx);

    std::memcpy(vec.data() + ptr, &net_last_log_term, sizeof(net_last_log_term));

};

void VecByteWriter::serialize(const InstallSnapshotReqPayload& payload) {
    auto msg_size               = htonl(payload.size() + sizeof(uint8_t));
    auto net_id                 = IS_RPC_ID;
    auto net_snapshot_len       = htonll(payload.snapshot.size());
    auto net_term               = htonl(payload.term);
    auto net_leader_id          = htonl(payload.leader_id);
    auto net_last_included_idx  = htonl(payload.last_included_idx);
    auto net_last_included_term = htonl(payload.last_included_term);
    auto net_offset             = htonl(payload.offset);
    auto net_done               = payload.done;

    vec.resize(offset + payload.size() + sizeof(msg_size) + sizeof(net_id));
    size_t ptr = offset;

    std::memcpy(vec.data() + ptr, &msg_size, sizeof(msg_size));
    ptr += sizeof(msg_size);

    std::memcpy(vec.data() + ptr, &net_id, sizeof(net_id));
    ptr += sizeof(net_id);

    std::memcpy(vec.data() + ptr, &net_snapshot_len, sizeof(net_snapshot_len));
    ptr += sizeof(net_snapshot_len);

    std::memcpy(vec.data() + ptr, payload.snapshot.data(), payload.snapshot.size());
    ptr += payload.snapshot.size();

    std::memcpy(vec.data() + ptr, &net_term, sizeof(net_term));
    ptr += sizeof(net_term);

    std::memcpy(vec.data() + ptr, &net_leader_id, sizeof(net_leader_id));
    ptr += sizeof(net_leader_id);

    std::memcpy(vec.data() + ptr, &net_last_included_idx, sizeof(net_last_included_idx));
    ptr += sizeof(net_last_included_idx);

    std::memcpy(vec.data() + ptr, &net_last_included_term, sizeof(net_last_included_term));
    ptr += sizeof(net_last_included_term);

    std::memcpy(vec.data() + ptr, &net_offset, sizeof(net_offset));
    ptr += sizeof(net_offset);

    std::memcpy(vec.data() + ptr, &net_done, sizeof(net_done));

};
