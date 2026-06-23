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
        inbox_.DrainAll([this, &leader_contact](std::unique_ptr<RaftMessage>&& message) {
            #ifdef DEBUG
            std::cout << "draining node inbox...\n";
            #endif
            VoidExpectedF ok = std::visit([this, &message, &leader_contact](auto&& payload) -> VoidExpectedF {
                using T = std::decay_t<decltype(payload)>;
                auto client_id = message->node_id;
                auto& el = loops_[client_id & (EVENT_LOOP_THREADS - 1)];

                /* Handlers */

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE RPC\n";
                    #endif

                    // reply false if:
                    // term < current_term
                    // log doesn't contain an entry at prev_log_index whose term matches prev_log_term

                    if (current_term_ > payload.term
                    || payload.prev_log_idx >= log_.size()
                    || log_[payload.prev_log_idx].term != payload.prev_log_term
                    ) {
                        el->outbound_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                AppendEntriesRespPayload{.term = current_term_, .success = 0}, client_id
                            )
                        );
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
                    if (state_ == NodeState::Leader) demote();
                    leader_contact = true;

                    el->outbound_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        AppendEntriesRespPayload{.success = 1}, client_id
                        )
                    );
                }

                else if constexpr (std::is_same_v<T, RequestVoteReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV RPC\n";
                    #endif

                    if (payload.term < current_term_ || voted_for_ != -1) {
                        el->outbound_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                RequestVoteRespPayload{current_term_, 0}, client_id
                            )
                        );
                        return {};
                    }

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        if (state_ == NodeState::Leader) demote();
                    }

                    leader_contact = true;
                    const uint32_t last_log_term = log_.back().term;
                    const uint32_t last_log_idx = log_.size() - 1;
                    if (payload.last_log_term > last_log_term
                    || (payload.last_log_term == last_log_term
                        && payload.last_log_idx >= last_log_idx))
                    {
                        voted_for_ = client_id;
                    }

                    el->outbound_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        RequestVoteRespPayload{current_term_, 1}, client_id
                        )
                    );
                    return {};
                }

                // TODO
                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC\n";
                    #endif

                    if (payload.term < current_term_) {
                        el->outbound_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            InstallSnapshotRespPayload{current_term_}, client_id)
                        );
                        return {};
                    }

                    current_term_ = payload.term;
                    if (state_ == NodeState::Leader) demote();
                    leader_contact = true;
                    // TODO: chunk reassembly, install snapshot to state machine.
                    el->outbound_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        InstallSnapshotRespPayload{current_term_}, client_id)
                    );
                }

                else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE reply\n";
                    #endif

                    const uint32_t last_log_idx = log_.size() - 1;
                    const auto it = std::find(node_ids_.begin(), node_ids_.end(), client_id);
                    if (it == node_ids_.end()) {
                        return UnexpectedF(
                            std::format("Failed to process AE reply: client ID {} not found in node_ids_", client_id)
                        );
                    }
                    const uint32_t stored_next = next_indexes_[it - node_ids_.begin()];
                    // next_idx = stored - 1 and prev_log_idx = next_idx - 1, so
                    // stored must be >= 2 to avoid uint32_t underflow.
                    if (stored_next < 2) {
                        return UnexpectedF(std::format(
                            "Failed to process AE reply: next_index {} for client_id {} too small to derive prev_log_idx",
                            stored_next, client_id
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
                                "Failed to process AE reply: failed to retry AE RPC to client_id {}; last_log_idx {} is less than decremented next_idx {}",
                                client_id, last_log_idx, next_idx
                            ));
                        };

                        auto entries_to_append = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);

                        send_rpc(AppendEntriesReqPayload{
                            entries_to_append,
                            current_term_,
                            MY_ID,
                            prev_log_idx,
                            prev_log_term,
                            commit_index_
                        }, client_id);

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

                    commit_if_quorum(match_indexes_, commit_index_, current_term_, log_);

                }

                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV reply\n";
                    #endif

                    if (state_ != NodeState::Candidate
                        || payload.term != current_term_
                        || !payload.vote_granted
                        || !voters_.insert(client_id).second
                    ) return UnexpectedF(std::format(
                        "Failed to process RequestVote reply: payload term ({}) either does not match current term of {}, or sender did not grant vote, or client_id {} has already voted for this node.",
                        payload.term, current_term_, client_id
                    ));

                    // become leader if quorum of votes achieved
                    if (voters_.size() + 1 > (node_ids_.size()+1) / 2) { // add 1 to account for this node
                        #ifdef DEBUG
                        std::cout << MY_ID << " won the election\n";
                        #endif
                        state_ = NodeState::Leader;
                        send_heartbeats_and_arm_timers();
                        voters_.clear();
                        voted_for_ = -1;

                        uint32_t last_log_idx = static_cast<uint32_t>(log_.size());
                        for (uint32_t& i : next_indexes_) {
                            i = last_log_idx + 1;
                        }
                        for (uint32_t& i : match_indexes_) {
                            i = 0;
                        }
                    }

                }

                // TODO
                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS reply\n";
                    #endif
                }

                // heartbeats are sent per follower, not all at once.
                else if constexpr (std::is_same_v<T, HeartbeatTimeoutPayload>) {
                    #ifdef DEBUG
                    std::cout << "found heartbeat timeout; sending heartbeats...\n";
                    #endif
                    // if last log index >= this follower's nextIndex,
                    // then send AE RPC w/ log entries starting at nextIndex. Otherwise, send term w/ no entries.
                    const auto it = std::find(node_ids_.begin(), node_ids_.end(), client_id);
                    if (it == node_ids_.end()) {
                        return UnexpectedF(std::format(
                            "Failed to process HeartbeatTimeout: client_id {} not found in node_ids_"
                            , client_id));
                    }
                    const uint32_t next_idx = next_indexes_[it - node_ids_.begin()];

                    // Only send entries when the log actually has some at/after
                    // next_idx. log_.size()-1 >= next_idx is restated as
                    // next_idx < log_.size() to avoid uint underflow on size 0.
                    if (next_idx < log_.size()) {
                        // prev_log_idx = next_idx - 1 requires next_idx >= 1.
                        if (next_idx == 0) {
                            return UnexpectedF(std::format(
                                "Failed to process HeartbeatTimeout: next_index 0 for client_id {} cannot derive prev_log_idx",
                                client_id
                            ));
                        }
                        const uint32_t prev_log_idx = next_idx - 1;
                        const uint32_t prev_log_term = log_[prev_log_idx].term;
                        auto s = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);
                        send_rpc(AppendEntriesReqPayload{
                            s,
                            current_term_,
                            MY_ID,
                            prev_log_idx,
                            prev_log_term,
                            commit_index_
                        }, client_id);
                    }
                    else {
                        send_rpc(AppendEntriesReqPayload{current_term_}, client_id);
                    }
                    el->Wake();
                }

                // These are control messages sent to event loops, not handled by Node.
                else if constexpr (std::is_same_v<T, ArmTimer> || std::is_same_v<T, DisArmTimer>) {}

                else {
                    static_assert(false, "non-exhaustive visitor");
                }
                return {};
            }, message->data);
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

        // poll the election timer
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_leader_contact_);

        // #ifdef DEBUG
        // std::cout << "duration = " << duration << "\n";
        // #endif

        if (duration < election_timeout_) continue;

        #ifdef DEBUG
        std::cout << "election timeout; starting election...\n";
        #endif

        // start election
        if (state_ == NodeState::Candidate) continue;
        state_ = NodeState::Candidate;
        ++current_term_;
        voted_for_ = MY_ID;
        last_leader_contact_ = std::chrono::steady_clock::now();

        for (NodeID id : node_ids_) {
            #ifdef DEBUG
            std::cout << "sending RV to peer " << id << " on event loop " << static_cast<int>(id & (EVENT_LOOP_THREADS - 1)) << "\n";
            #endif

            auto& el = loops_[id & (EVENT_LOOP_THREADS - 1)];
            el->outbound_inbox.PushOne(
                std::make_unique<RaftMessage>(RequestVoteReqPayload{
                    current_term_,
                    MY_ID,
                    static_cast<uint32_t>(log_.size() - 1),
                    log_.back().term
                }, id)
            );

            el->Wake();
        }
    }
}
