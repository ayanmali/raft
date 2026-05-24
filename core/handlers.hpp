
// ---- inbound handlers ------------------------------------------------------
#include "node.hpp"
#include "../rpc/protocol/utils.hpp"
#include <mutex>

// template <uint N>
// inline RpcHandlers Node<N>::make_handlers() {
//     RpcHandlers h;
//     h.on_ae_req = [this](const AppendEntriesReqPayload& p) {
//         return handle_append_entries(p);
//     };
//     h.on_rv_req = [this](const RequestVoteReqPayload& p) {
//         return handle_request_vote(p);
//     };
//     h.on_is_req = [this](const InstallSnapshotReqPayload& p) {
//         return handle_install_snapshot(p);
//     };
//     h.on_peer_tick = [this](NodeID peer_id) { tick_peer(peer_id); };
//     return h;
// }

// TODO: Stubs for now -- full Raft state machine logic (log matching,
// commit advancement, vote rules, snapshot install) is out of scope
// for this commit. Each handler acquires state_mu_, makes the
// minimal term-bumping decision, and returns a wire-formed reply.

template <uint N>
std::expected<RpcReply, const char*> Node<N>::handle_append_entries_req(const RpcMessage& message) {
    try {
        const AppendEntriesReqPayload req{std::get<AppendEntriesReqPayload>(message)};

        std::lock_guard<std::mutex> lk(state_mu_);

        if (req.term < current_term) {
            return AppendEntriesRespPayload{current_term, 0};
        }

        if (req.term > current_term) {
            current_term = req.term;
            voted_for    = -1;
            leader.store(false, std::memory_order_release);
        }

        // TODO: log matching, append, leader_commit advancement.
        return AppendEntriesRespPayload{static_cast<uint32_t>(current_term), 1};
    }
    catch (std::exception&) {
        return std::unexpected("failed to get AE payload from variant");
    }
}

template <uint N>
inline std::expected<RpcReply, const char*> Node<N>::handle_request_vote_req(const RpcMessage& message) {
    try {
        const RequestVoteReqPayload req{std::get<RequestVoteReqPayload>(message)};

        std::lock_guard<std::mutex> lk(state_mu_);

        if (req.term < current_term) {
            return RequestVoteRespPayload{current_term, 0};
        }
        if (req.term > current_term) {
            current_term = req.term;
            leader       = false;
        }

        uint8_t granted = 0;
        // if (voted_for == req.candidate_id) {
        //     // TODO: log up-to-date check (last_log_idx/last_log_term).
        //     voted_for = req.candidate_id;
        //     granted   = 1;
        // }

        return RequestVoteRespPayload{current_term, granted};
    }
    catch (std::exception&) {
        return std::unexpected("failed to get RV payload from variant");
    }
}

template <uint N>
inline std::expected<RpcReply, const char*> Node<N>::handle_install_snapshot_req(const RpcMessage& message) {
    InstallSnapshotReqPayload req;
    try {
        const InstallSnapshotReqPayload req{std::get<InstallSnapshotReqPayload>(message)};

        std::lock_guard<std::mutex> lk(state_mu_);

        if (req.term < current_term) {
            return InstallSnapshotRespPayload{static_cast<uint32_t>(current_term)};
        }

        if (req.term > current_term) {
            current_term = req.term;
            leader       = false;
        }

        // TODO: chunk reassembly, install snapshot to state machine.
        return InstallSnapshotRespPayload{current_term};
    }
    catch (std::exception&) {
        return std::unexpected("failed to get IS payload from variant");
    }

}

template <uint N>
inline void Node<N>::handle_append_entries_reply(const RpcReply& reply_raw) {
    try {
        const AppendEntriesRespPayload reply{std::get<AppendEntriesRespPayload>(reply_raw)};
        
        std::lock_guard<std::mutex> lk(state_mu_);

        // TODO
    }
    catch (std::exception&) {
        return;
    }
}


template <uint N>
inline void Node<N>::handle_request_vote_reply(const RpcReply& reply_raw) {
    try {
        const RequestVoteRespPayload reply{std::get<RequestVoteRespPayload>(reply_raw)};
        
        std::lock_guard<std::mutex> lk(state_mu_);

        // TODO
    }
    catch (std::exception&) {
        return;
    }
}


template <uint N>
inline void Node<N>::handle_install_snapshot_reply(const RpcReply& reply_raw) {
    try {
        const InstallSnapshotRespPayload reply{std::get<InstallSnapshotRespPayload>(reply_raw)};
        
        std::lock_guard<std::mutex> lk(state_mu_);

        // TODO
    }
    catch (std::exception&) {
        return;
    }
}

template <uint N>
constexpr std::array<HandlerFunc, 4> make_req_handler_table() {
    std::array<HandlerFunc, 4> table{};
    table[1] = Node<N>::handle_append_entries_req;
    table[2] = Node<N>::handle_request_vote_req;
    table[3] = Node<N>::handle_install_snapshot_req;

    return table;
}

template <uint N>
constexpr std::array<HandlerFunc, 4> make_reply_handler_table() {
    std::array<HandlerFunc, 4> table{};
    table[1] = Node<N>::handle_append_entries_reply;
    table[2] = Node<N>::handle_request_vote_reply;
    table[3] = Node<N>::handle_install_snapshot_reply;

    return table;
}

template <uint N>
constexpr auto REQ_HANDLER_TABLE = make_req_handler_table<N>();

template <uint N>
constexpr auto REPLY_HANDLER_TABLE = make_reply_handler_table<N>();

template <uint N>
inline std::expected<RpcReply, const char*> Node<N>::handle_request(const RpcMessage& req, uint8_t rpc_id) {
    return REQ_HANDLER_TABLE<N>[rpc_id](req);
}

template <uint N>
inline void handle_reply(const RpcReply& reply, uint8_t rpc_id) {
    return REPLY_HANDLER_TABLE<N>[rpc_id](reply);
}