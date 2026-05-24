#include "../conns.hpp"
#include "payloads.hpp"
#include <expected>

/* Inbound */

// inline std::pair<size_t, AppendEntriesReqPayload> parse_ae(const std::byte* in);
// inline std::pair<size_t, RequestVoteReqPayload> parse_rv(const std::byte* in);
// inline std::pair<size_t, InstallSnapshotReqPayload> parse_is(const std::byte* in);

inline std::expected<RpcMessage, const char*> parse_ae_req(ByteReader& byte_reader) {
    AppendEntriesReqPayload message;

    if (!byte_reader.read(message.entries)) return std::unexpected("failed to parse AppendEntries entries field");
    if (!byte_reader.read(message.term)) return std::unexpected("failed to parse AppendEntries term field");
    if (!byte_reader.read(message.leader_id)) return std::unexpected("failed to parse AppendEntries leader_id field");
    if (!byte_reader.read(message.prev_log_idx)) return std::unexpected("failed to parse AppendEntries prev_log_idx field");
    if (!byte_reader.read(message.prev_log_term)) return std::unexpected("failed to parse AppendEntries prev_log_term field");
    if (!byte_reader.read(message.leader_commit)) return std::unexpected("failed to parse AppendEntries leader_commit field");

    return message;
}


inline std::expected<RpcMessage, const char*> parse_rv_req(ByteReader& byte_reader) {
    RequestVoteReqPayload message;

    if (!byte_reader.read(message.term)) return std::unexpected("failed to parse RequestVote term field");
    if (!byte_reader.read(message.candidate_id)) return std::unexpected("failed to parse RequestVote candidate_id field");
    if (!byte_reader.read(message.last_log_idx)) return std::unexpected("failed to parse RequestVote last_log_idx field");
    if (!byte_reader.read(message.last_log_term)) return std::unexpected("failed to parse RequestVote last_log_term field");

    return message;
}


inline std::expected<RpcMessage, const char*> parse_is_req(ByteReader& byte_reader) {
    InstallSnapshotReqPayload message;

    if (!byte_reader.read(message.snapshot)) return std::unexpected("failed to parse InstallSnapshot snapshot field");
    if (!byte_reader.read(message.term)) return std::unexpected("failed to parse InstallSnapshot term field");
    if (!byte_reader.read(message.leader_id)) return std::unexpected("failed to parse InstallSnapshot leader_id field");
    if (!byte_reader.read(message.last_included_idx)) return std::unexpected("failed to parse InstallSnapshot last_included_idx field");
    if (!byte_reader.read(message.last_included_term)) return std::unexpected("failed to parse InstallSnapshot last_included_term field");
    if (!byte_reader.read(message.offset)) return std::unexpected("failed to parse InstallSnapshot offset field");
    if (!byte_reader.read(message.done)) return std::unexpected("failed to parse InstallSnapshot done field");

    return message;
}

constexpr std::array<ParserFunc, 4> make_parser_table() {
    std::array<ParserFunc, 4> table{};
    table[1] = parse_ae_req;
    table[2] = parse_rv_req;
    table[3] = parse_is_req;

    return table;
}

constexpr auto PARSER_TABLE = make_parser_table();

inline std::pair<std::expected<RpcMessage, const char*>, uint8_t> parse_rbuf(ClientConn& c) {
    if (c.rbuf.size() < sizeof(uint32_t)) return {std::unexpected("not enough data to read"), 0}; // need to see message size first
    uint32_t message_size;
    std::memcpy(&message_size, c.rbuf.data(), sizeof(message_size));
    message_size = ntohl(message_size);

    ByteReader byte_reader(std::span<std::byte>(c.rbuf.begin(), c.rbuf.begin() + message_size));
    uint8_t rpc_id;

    if (!byte_reader.read(rpc_id)) return {std::unexpected("failed to parse RPC id"), 0};

    auto func = PARSER_TABLE[rpc_id];
    if (!func) return {std::unexpected("invalid RPC id"), 0};

    c.rbuf.erase(c.rbuf.begin(), c.rbuf.begin() + sizeof(message_size) + sizeof(rpc_id));
    return {func(byte_reader), rpc_id};
}

/* Outbound */

struct ClientReplyVisitor {
    public:
    ClientReplyVisitor(std::vector<std::byte>& buf_) : buf(buf_) {}

    void operator()(const AppendEntriesRespPayload& reply) {
        buf.reserve(reply.size());

        size_t ptr = 0;
        auto msg_size = reply.size();
        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);
        std::memcpy(buf.data() + ptr, &reply.term, sizeof(reply.term));
        ptr += sizeof(reply.term);
        std::memcpy(buf.data() + ptr, &reply.success, sizeof(reply.success));
    }
    void operator()(const RequestVoteRespPayload& reply) {
        buf.reserve(reply.size());

        size_t ptr = 0;
        auto msg_size = reply.size();
        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);
        std::memcpy(buf.data() + ptr, &reply.term, sizeof(reply.term));
        ptr += sizeof(reply.term);
        std::memcpy(buf.data() + ptr, &reply.vote_granted, sizeof(reply.vote_granted));
    }
    void operator()(const InstallSnapshotRespPayload& reply) {
        buf.reserve(reply.size());

        size_t ptr = 0;
        auto msg_size = reply.size();
        std::memcpy(buf.data(), &msg_size, sizeof(msg_size));
        ptr += sizeof(msg_size);
        std::memcpy(buf.data() + ptr, &reply.term, sizeof(reply.term));
    }

    private:
    std::vector<std::byte>& buf;
};

inline void ClientConn::write_reply(RpcReply& reply) {
    std::visit(ClientReplyVisitor{wbuf}, reply);
}