#include "./node.hpp"
#include <algorithm>
#ifdef DEBUG
#include <iostream>
#include <chrono>
#endif

void Node::MainLoop() {
    #ifdef DEBUG
    std::cout << "starting node loop (main thread)\n";
    #endif
    while (true) {
        // check the reply inbox for new replies that have arrived
        bool leader_contact{false};
        inbox_.DrainAll([this, &leader_contact](std::unique_ptr<RpcMessage>&& message) {
            #ifdef DEBUG
            std::cout << "draining node inbox...\n";
            #endif
            VoidExpectedF ok = std::visit([this, &message, &leader_contact](auto&& payload) -> VoidExpectedF {
                using T = std::decay_t<decltype(payload)>;

                /* Handlers */

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE RPC from node " << payload.leader_id << "\n";
                    std::cout << "payload term = " << payload.term << "\n";
                    std::cout << "current term = " << current_term_ << "\n";
                    std::cout << "prev log index = " << payload.prev_log_idx << "\n";
                    std::cout << "prev log term = " << payload.prev_log_term << "\n";
                    std::cout << "commit index = " << payload.leader_commit << "\n";

                    std::cout << "next indexes: ";
                    for (auto n : next_indexes_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";

                    std::cout << "match indexes: ";
                    for (auto n : match_indexes_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";

                    #endif
                    auto& el = loops_[payload.leader_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.leader_id, payload.fd, el);

                    // reply false if:
                    // term < current_term
                    // log doesn't contain an entry at prev_log_index whose term matches prev_log_term

                    if (current_term_ > payload.term
                    || payload.prev_log_idx >= log_.size()
                    || log_[payload.prev_log_idx].term != payload.prev_log_term
                    ) {
                        send(AppendEntriesRespPayload{
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .term = current_term_,
                            .success = 0}, el);
                        return {};
                    }

                    // if an existing entry conflicts w/ a new one (same index but
                    // different terms), delete the existing entry and all that
                    // follow it. The scan is bounded by BOTH the number of local
                    // log entries after prev_log_idx and the number of incoming
                    // entries, so neither log_ nor payload.entries is indexed OOB.
                    const size_t existing_after_prev = log_.size() - payload.prev_log_idx - 1;
                    const size_t scan_limit = std::min(existing_after_prev, payload.entries.size());
                    size_t i = 1;
                    for (; i < scan_limit; ++i) {
                        const size_t log_idx = payload.prev_log_idx + i + 1;
                        if (log_[log_idx].term != payload.entries[i].term) {
                            // conflict: truncate the local log from this index on.
                            log_.erase(log_.begin() + log_idx, log_.end());
                            break;
                        }
                    }

                    // append any entries not already in the log
                    log_.reserve(payload.entries.size());
                    for (auto it = payload.entries.begin() + i; it < payload.entries.end(); ++it) {
                        log_.emplace_back(std::move(it->data), it->term);
                    }

                    if (payload.leader_commit > commit_index_) {
                        commit_index_ = std::min(payload.leader_commit, static_cast<uint32_t>(payload.prev_log_idx + payload.entries.size()));
                    }

                    current_term_ = payload.term;
                    if (state_ != NodeState::Follower) demote();
                    leader_contact = true;

                    send(AppendEntriesRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .success = 1}, el);
                }

                else if constexpr (std::is_same_v<T, RequestVoteReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV RPC from node " << payload.candidate_id << "\n";
                    #endif
                    auto& el = loops_[payload.candidate_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.candidate_id, payload.fd, el);

                    #ifdef DEBUG
                    std:: cout << "current cluster: " << MY_ID << ", ";
                    for (auto n : node_ids_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";

                    std::cout << "Payload term = " << payload.term << ", this node's term = " << current_term_ << "\n";
                    std::cout << "This node's voted_for = " << voted_for_ << "\n";
                    std::cout << "This node's voters = ";
                    for (auto v : voters_) {
                        std::cout << v << ", ";
                    }
                    std::cout << "\n";

                    #endif

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demote();
                    }

                    if (payload.term < current_term_ || voted_for_ != -1) {
                        #ifdef DEBUG
                        std::cout << "rejecting RV from node " << payload.candidate_id << "\n";
                        #endif
                        send(RequestVoteRespPayload{
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .term = current_term_,
                            .vote_granted = 0}, el);
                        return {};
                    }

                    leader_contact = true;
                    const uint32_t last_log_term = log_.back().term;
                    const uint32_t last_log_idx = log_.size() - 1;
                    if (payload.last_log_term > last_log_term
                    || (payload.last_log_term == last_log_term
                        && payload.last_log_idx >= last_log_idx))
                    {
                        voted_for_ = payload.candidate_id;
                    }

                    send(RequestVoteRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_,
                        .vote_granted = 1}, el);

                    #ifdef DEBUG
                    std::cout << "voting for node " << payload.candidate_id << "\n";
                    #endif
                    return {};
                }

                // TODO
                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC from node " << payload.leader_id << "\n";
                    #endif
                    auto& el = loops_[payload.leader_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.leader_id, payload.fd, el);

                    if (payload.term < current_term_) {
                        send(InstallSnapshotRespPayload{
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .term = current_term_}, el);
                        return {};
                    }

                    current_term_ = payload.term;
                    if (state_ == NodeState::Leader) demote();
                    leader_contact = true;
                    // TODO: chunk reassembly, install snapshot to state machine.
                    send(InstallSnapshotRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_}, el);
                }

                else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE reply from node " << payload.server_id << "\n";
                    std::cout << "server term = " << payload.term << "\n";
                    std::cout << "current term = " << current_term_ << "\n";
                    std::cout << "success = " << static_cast<int>(payload.success) << "\n";

                    std::cout << "next indexes: ";
                    for (auto n : next_indexes_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";

                    std::cout << "match indexes: ";
                    for (auto n : match_indexes_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";
                    #endif

                    const uint32_t last_log_idx = log_.size() - 1;
                    const auto it = std::find(node_ids_.begin(), node_ids_.end(), payload.server_id);
                    if (it == node_ids_.end()) {
                        return UnexpectedF(
                            std::format("Failed to process AE reply: node ID {} not found in node_ids_", payload.server_id)
                        );
                    }
                    const uint32_t stored_next = next_indexes_[it - node_ids_.begin()];
                    // next_idx = stored - 1 and prev_log_idx = next_idx - 1, so
                    // stored must be >= 2 to avoid uint32_t underflow.
                    if (stored_next < 2) {
                        return UnexpectedF(std::format(
                            "Failed to process AE reply: next_index {} for server id {} too small to derive prev_log_idx",
                            stored_next, payload.server_id
                        ));
                    }
                    const uint32_t next_idx = stored_next - 1;
                    const uint32_t prev_log_idx = next_idx - 1;
                    if (prev_log_idx >= log_.size()) {
                        return UnexpectedF(std::format(
                            "Failed to process AE reply: prev_log_idx {} out of bounds (log size {})",
                            prev_log_idx, log_.size()
                        ));
                    }
                    const uint32_t prev_log_term = log_[prev_log_idx].term;

                    /*
                     on fail:
                     decrement nextIndex and retry
                    */
                    if (payload.success == 0) {
                        if (last_log_idx < next_idx) {
                            return UnexpectedF(std::format(
                                "Failed to process AE reply: failed to retry AE RPC to server id {}; last_log_idx {} is less than decremented next_idx {}",
                                payload.server_id, last_log_idx, next_idx
                            ));
                        };

                        auto entries_to_append = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);

                        auto& el = loops_[payload.server_id & (EVENT_LOOP_THREADS - 1)];
                        send(AppendEntriesReqPayload{
                            entries_to_append,
                            payload.server_id,
                            current_term_,
                            MY_ID,
                            prev_log_idx,
                            prev_log_term,
                            commit_index_
                        }, el);

                        return {};
                    }

                    /*
                     on success:
                     - nextIndex is updated to prevLogIndex + 1 + len(entries)
                     (or simply incremented by the number of replicated entries).

                     - matchIndex is set to the index of the last entry successfully
                     appended (calculated as prevLogIndex + len(entries))
                     */
                    next_indexes_[it - node_ids_.begin()] += payload.entries_len;
                    match_indexes_[it - node_ids_.begin()] = prev_log_idx + payload.entries_len;

                    /*
                     if there exists an N such that
                     N > commit_index,
                     a majority of matchIndex[N] >= N,
                     and log[N].term == currentTerm,
                     set commitIndex = N

                     i.e. if a majority of servers have a matchIndex greater than or equal to index N,
                     and the entry at N is in the current term,
                     then set commitIndex to N
                     --> entries <= commitIndex become committed. TODO: Send confirmation to client
                     */

                    commit_if_quorum(commit_index_);

                }

                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV reply from node " << payload.server_id << "\n";
                    std:: cout << "current cluster: " << MY_ID << ", ";
                    for (auto n : node_ids_) {
                        std::cout << n << ", ";
                    }
                    std::cout << "\n";

                    std::cout << "Payload term = " << payload.term << ", this node's term = " << current_term_ << "\n";
                    std::cout << "vote granted = " << static_cast<int>(payload.vote_granted) << "\n";
                    std::cout << "This node's voted_for = " << voted_for_ << "\n";
                    std::cout << "This node's voters = ";
                    for (auto v : voters_) {
                        std::cout << v << ", ";
                    }
                    std::cout << "\n";
                    #endif

                    if (state_ != NodeState::Candidate
                        || payload.term != current_term_
                        || payload.vote_granted == 0
                        || !voters_.insert(payload.server_id).second
                    ) {
                        #ifdef DEBUG
                        std::cout << "payload term " << payload.term << " does not match current term " << current_term_ << ", or sender did not grant vote, or server id " << payload.server_id << " has already voted for this node; skipping\n";
                        #endif
                        return {};
                    }

                    // become leader if quorum of votes achieved
                    if (voters_.size() + 1 > (node_ids_.size() + 1) / 2) {
                        become_leader();
                    }
                    return {};
                }

                // TODO
                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS reply from node " << payload.server_id << "\n";
                    #endif
                }

                // heartbeats are sent per follower, not all at once.
                else if constexpr (std::is_same_v<T, HeartbeatTimeout>) {
                    #ifdef DEBUG
                    std::cout << "found heartbeat timeout for node " << payload.source_id << "; sending heartbeat...\n";
                    #endif
                    // if last log index >= this follower's nextIndex,
                    // then send AE RPC w/ log entries starting at nextIndex. Otherwise, send term w/ no entries.
                    const auto it = std::find(node_ids_.begin(), node_ids_.end(), payload.source_id);
                    if (it == node_ids_.end()) {
                        return UnexpectedF(std::format(
                            "Failed to process HeartbeatTimeout: node id {} not found in node_ids_"
                            , payload.source_id));
                    }
                    const uint32_t next_idx = next_indexes_[it - node_ids_.begin()];

                    // Only send entries when the log actually has some at/after
                    // next_idx. log_.size()-1 >= next_idx is restated as
                    // next_idx < log_.size() to avoid uint underflow on size 0.
                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    if (next_idx < log_.size()) {
                        // prev_log_idx = next_idx - 1 requires next_idx >= 1.
                        if (next_idx == 0) {
                            return UnexpectedF(std::format(
                                "Failed to process HeartbeatTimeout: next_index 0 for node id {} cannot derive prev_log_idx",
                                payload.source_id
                            ));
                        }
                        const uint32_t prev_log_idx = next_idx - 1;
                        const uint32_t prev_log_term = log_[prev_log_idx].term;
                        auto s = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);
                        send(AppendEntriesReqPayload{
                            s,
                            payload.source_id,
                            current_term_,
                            MY_ID,
                            prev_log_idx,
                            prev_log_term,
                            commit_index_
                        }, el);
                    }
                    else {
                        send(AppendEntriesReqPayload{
                            current_term_,
                            MY_ID,
                            payload.source_id
                        }, el);
                    }

                }

                else if constexpr (std::is_same_v<T, DropPeerMsg>) {
                    #ifdef DEBUG
                    std::cout << "Received drop peer message - dropping peer " << payload.source_id << "\n";
                    #endif
                    if (auto it = std::find(node_ids_.begin(), node_ids_.end(), payload.source_id); it != node_ids_.end() ) {
                        size_t index = std::distance(node_ids_.begin(), it);
                        node_ids_.erase(it);
                        next_indexes_.erase(next_indexes_.begin() + index);
                        match_indexes_.erase(match_indexes_.begin() + index);
                    }
                    voters_.erase(payload.source_id);
                    if (voted_for_ == payload.source_id) {
                        voted_for_ = -1;
                    }
                }

                // These are control messages sent to event loops, not handled by Node.
                else if constexpr (std::is_same_v<T, ArmTimer>
                    || std::is_same_v<T, DisarmTimer>
                    || std::is_same_v<T, AddPeerMsg>) {}

                else {
                    static_assert(false, "non-exhaustive visitor");
                }
                return {};
            }, *message.get());
            #ifdef DEBUG
            if (!ok) std::cout << "inbox handler error: " << ok.error() << "\n";
            #else
            (void)ok;
            #endif
        });

        if (state_ == NodeState::Leader) continue;

        last_leader_contact_ = leader_contact
            ? std::chrono::steady_clock::now() // reset only if a leader message came in
            : last_leader_contact_;

        #ifdef DEBUG
        if (leader_contact) {
            std::cout << "Leader discovered at a different node\n";
        }
        #endif

        // poll the election timer
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_leader_contact_);

        // #ifdef DEBUG
        // std::cout << "duration = " << duration << "\n";
        // #endif

        if (duration < election_timeout_) continue;

        // start election
        if (state_ == NodeState::Candidate) continue;
        #ifdef DEBUG
        std::cout << "election timeout; starting election...\n";
        #endif

        state_ = NodeState::Candidate;
        ++current_term_;
        voted_for_ = MY_ID;
        last_leader_contact_ = std::chrono::steady_clock::now();

        // A node always votes for itself. If that single vote is already a
        // majority (e.g. a single-node cluster with no peers), win the
        // election immediately rather than waiting for RequestVote replies
        // that will never come.
        if (voters_.size() + 1 > (node_ids_.size() + 1) / 2) {
            become_leader();
            continue;
        }
        request_votes();
    }
}
