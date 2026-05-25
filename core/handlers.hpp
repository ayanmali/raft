
// ---- inbound handlers ------------------------------------------------------
#include "node.hpp"
#include "../rpc/protocol/utils.hpp"

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
struct RequestHandlerVisitor {
    public:
    RequestHandlerVisitor(Node<N>* node_) : node{node_} {}

    void operator()(const AppendEntriesReqPayload& req) {
        if (req.term < node->current_term) {
            return AppendEntriesRespPayload{node->current_term, 0};
        }

        if (req.term > node->current_term) {
            node->current_term = req.term;
            node->voted_for    = -1;
            node->leader.store(false, std::memory_order_release);
        }
    }

    void operator()(const RequestVoteReqPayload& req) {

        if (req.term < node->current_term) {
            return RequestVoteRespPayload{node->current_term, 0};
        }
        if (req.term > node->current_term) {
            node->current_term = req.term;
            node->leader       = false;
        }

        uint8_t granted = 0;
        // if (voted_for == req.candidate_id) {
        //     // TODO: log up-to-date check (last_log_idx/last_log_term).
        //     voted_for = req.candidate_id;
        //     granted   = 1;
        // }

        return RequestVoteRespPayload{node->current_term, granted};
    }

    void operator()(const InstallSnapshotReqPayload& req) {
        if (req.term < node->current_term) {
            return InstallSnapshotRespPayload{static_cast<uint32_t>(current_term)};
        }

        if (req.term > node->current_term) {
            node->current_term = req.term;
            node->leader       = false;
        }

        // TODO: chunk reassembly, install snapshot to state machine.
        return InstallSnapshotRespPayload{node->current_term};
    }
    // these should be unreachable
    void operator()(const ArmTimerPayload& req) {}
    void operator()(const DisarmTimerPayload& req) {}

    private:
    Node<N>* node;
};

// TODO
template <uint N>
struct ReplyHandlerVisitor {
    public:
    ReplyHandlerVisitor(Node<N>* node_) : node{node_} {};

    void operator()(const AppendEntriesRespPayload& reply) {

    }

    void operator()(const RequestVoteRespPayload& reply) {

    }

    void operator()(const InstallSnapshotRespPayload& reply) {

    }
    private:
    Node<N>* node;
};

template <uint N>
inline std::expected<RpcReply, const char*> Node<N>::handle_request(const RpcMessage& req) {
    std::visit(RequestHandlerVisitor{this}, req);
}

template <uint N>
inline void Node<N>::handle_reply(const RpcReply& reply) {
    std::visit(ReplyHandlerVisitor{this}, reply);
}