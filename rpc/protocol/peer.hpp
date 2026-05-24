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

/* Outbound */
// TODO: replace with writev/readv. Requires event loop and client connection refactor
struct RPCWriter {
    public:
    RPCWriter(std::vector<std::byte>& buf_) : buf{buf_} {}

    void serialize(const AppendEntriesReqPayload& payload) {
        auto msg_size         = htonll(payload.size());
        auto net_id           =  htons(AE_RPC_ID);
        auto net_term         =  htonl(payload.term);
        auto net_leader_id    =  htonl(payload.leader_id);
        auto net_prev_log_idx =  htonl(payload.prev_log_idx);
        auto net_prev_log_term=  htonl(payload.prev_log_term);
        auto net_leader_commit=  htonl(payload.leader_commit);
        auto net_entries_len  = htonll(payload.entries.size());

        buf.reserve(msg_size);
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        ptr += sizeof(net_id);

        std::memcpy(buf.data() + ptr, &net_term, sizeof(net_term));
        ptr += sizeof(net_term);

        std::memcpy(buf.data() + ptr, &net_leader_id, sizeof(net_leader_id));
        ptr += sizeof(net_leader_id);

        std::memcpy(buf.data() + ptr, &net_prev_log_idx, sizeof(net_prev_log_idx));
        ptr += sizeof(net_prev_log_idx);

        std::memcpy(buf.data() + ptr, &net_prev_log_term, sizeof(net_prev_log_term));
        ptr += sizeof(net_prev_log_term);

        std::memcpy(buf.data() + ptr, &net_leader_commit, sizeof(net_leader_commit));
        ptr += sizeof(net_leader_commit);

        std::memcpy(buf.data() + ptr, &net_entries_len, sizeof(net_entries_len));
        ptr += sizeof(net_entries_len);

        std::memcpy(buf.data() + ptr, payload.entries.data(), payload.entries.size());
    
    };

    void serialize(const RequestVoteReqPayload& payload) {
        auto msg_size          = htonll(payload.size());
        auto net_id            = htons (RV_RPC_ID);
        auto net_term          = htonl(payload.term);
        auto net_candidate_id  = htonl(payload.candidate_id);
        auto net_last_log_idx  = htonl(payload.last_log_idx);
        auto net_last_log_term = htonl(payload.last_log_term);

        buf.reserve(payload.size());
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
        auto net_id                 = htons(IS_RPC_ID);
        auto net_term               = htonl(payload.term);
        auto net_leader_id          = htonl(payload.leader_id);
        auto net_last_included_idx  = htonl(payload.last_included_idx);
        auto net_last_included_term = htonl(payload.last_included_term);
        auto net_offset             = htonl(payload.offset);
        auto net_done               = htons(payload.done);
        auto net_snapshot_len       = htonll(payload.snapshot.size());
        
        buf.reserve(payload.size());
        size_t ptr = 0;

        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);

        std::memcpy(buf.data() + ptr, &net_id, sizeof(net_id));
        ptr += sizeof(net_id);

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
        ptr += sizeof(net_done);

        std::memcpy(buf.data() + ptr, &net_snapshot_len, sizeof(net_snapshot_len));
        ptr += sizeof(net_snapshot_len);

        std::memcpy(buf.data() + ptr, payload.snapshot.data(), payload.snapshot.size());
    
    };
    
    private:
    std::vector<std::byte>& buf;
};
