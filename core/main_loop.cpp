#include "./node.hpp"
#ifdef DEBUG
#include <iostream>
#endif

void Node::MainLoop() {
    #ifdef DEBUG
    std::cout << "starting node loop (main thread)\n";
    #endif
    while (true) {
        // check the reply inbox for new replies that have arrived
        // TODO: implement handlers
        bool leader_contact{false};
        bool demoted{false};
        inbox_.DrainAll([this, &leader_contact, &demoted](std::unique_ptr<RaftMessage>&& message) {
            #ifdef DEBUG
            std::cout << "draining node inbox...\n";
            #endif
            std::visit([this, &message, &leader_contact, &demoted](auto&& payload) {
                using T = std::decay_t<decltype(payload)>;
                auto client_id = message->node_id;
                auto& el_inbox = loops_[client_id & (EVENT_LOOP_THREADS - 1)]->outbound_inbox;

                /* Handlers */

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE RPC\n";
                    #endif

                    if (current_term_ > payload.term) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                AppendEntriesRespPayload{.term = current_term_, .success = 0}, client_id
                            )
                        );
                    }
                    current_term_ = payload.term;
                    voted_for_ = -1;
                    demoted = demoted || state_ == NodeState::Leader;
                    state_ = NodeState::Follower;
                    leader_contact = true;
                    el_inbox.PushOne(
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
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                                RequestVoteRespPayload{current_term_, 0}, client_id
                            )
                        );
                        return;
                    }

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demoted = demoted || state_ == NodeState::Leader;
                        state_ = NodeState::Follower;
                        voted_for_ = -1;
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

                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        RequestVoteRespPayload{current_term_, 1}, client_id
                        )
                    );
                }

                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC\n";
                    #endif

                    if (payload.term < current_term_) {
                        el_inbox.PushOne(
                            std::make_unique<RaftMessage>(
                            InstallSnapshotRespPayload{current_term_}, client_id)
                        );
                        return;
                    }

                    current_term_ = payload.term;
                    demoted = demoted || state_ == NodeState::Leader;
                    state_ = NodeState::Follower;
                    voted_for_ = -1;
                    leader_contact = true;
                    // TODO: chunk reassembly, install snapshot to state machine.
                    el_inbox.PushOne(
                        std::make_unique<RaftMessage>(
                        InstallSnapshotRespPayload{current_term_}, client_id)
                    );
                }

                else if constexpr (std::is_same_v<T, AppendEntriesRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE reply\n";
                    #endif

                    const uint32_t last_log_idx = log_.size() - 1;
                    auto it = std::find(node_ids_.begin(), node_ids_.end(), client_id);
                    const uint32_t next_idx = next_index_[it - node_ids_.begin()] - 1;
                    const uint32_t prev_log_idx = next_idx > 0 ? next_idx - 1 : 0;
                    const uint32_t prev_log_term = log_[prev_log_idx].term;

                    /*
                     on fail:
                     decrement nextIndex and retry
                    */
                    if (payload.success == 0) {
                        if (last_log_idx < next_idx) return; // TODO: how should this be handled?

                        auto entries_to_append = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);

                        send_rpc(AppendEntriesReqPayload{
                            entries_to_append,
                            current_term_,
                            MY_ID,
                            prev_log_idx,
                            prev_log_term,
                            commit_index_
                        }, client_id);

                        return;
                    }

                    /*
                     on success:
                     - nextIndex is updated to prevLogIndex + 1 + len(entries)
                     (or simply incremented by the number of replicated entries).

                     - matchIndex is set to the index of the last entry successfully
                     appended (calculated as prevLogIndex + len(entries))
                     */
                    next_index_[it - node_ids_.begin()] += payload.entries_len;
                    match_index_[it - node_ids_.begin()] = prev_log_idx + payload.entries_len;

                    /*
                     TODO:
                     if there exists an N such that
                     N > commit_index,
                     a majority of matchIndex[N] >= N,
                     and log[N].term == currentTerm,
                     set commitIndex = N

                     i.e. if a majority of servers have a matchIndex greater than or equal to index N,
                     and the entry at N is in the current term,
                     then set commitIndex to N
                     --> entries <= commitIndex become committed. Send confirmation to client
                     */

                    auto map = std::unordered_map<uint32_t, uint32_t>(match_index_.size());
                    for (auto& match_idx : match_index_) {
                        map[match_idx]++;
                    }
                    for (auto [match_idx, count] : map) {
                        if (count >= match_index_.size() / 2 && match_idx > commit_index_ && log_[match_idx].term == current_term_) {
                            commit_index_ = match_idx;
                            break;
                        }
                    }
                }

                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV reply\n";
                    #endif

                    if (state_ != NodeState::Candidate
                        || payload.term != current_term_
                        || !payload.vote_granted
                        || !voters_.insert(client_id).second
                    ) return;

                    // become leader if quorum of votes achieved
                    if (voters_.size() + 1 >= (node_ids_.size()+1) / 2) { // add 1 to account for this node
                        state_ = NodeState::Leader;
                        send_heartbeats_and_arm_timers();
                        voters_.clear();
                        voted_for_ = -1;

                        uint32_t last_log_idx = static_cast<uint32_t>(log_.size());
                        for (auto& i : next_index_) {
                            i = last_log_idx + 1;
                        }
                        for (auto& i : match_index_) {
                            i = 0;
                        }
                    }

                }

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
                    const uint32_t next_idx = next_index_[it - node_ids_.begin()];
                    auto s = std::span<LogEntry>(log_.data() + next_idx, log_.size() - next_idx);
                    const uint32_t prev_log_idx = next_idx > 0 ? next_idx - 1 : 0;
                    const uint32_t prev_log_term = log_[prev_log_idx].term;

                    if (log_.size() - 1 >= next_idx) {
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
                    loops_[client_id & (EVENT_LOOP_THREADS - 1)]->Wake();
                }

                // These are control messages sent to event loops, not handled by Node.
                else if constexpr (std::is_same_v<T, ArmTimer> || std::is_same_v<T, DisArmTimer>) {}

                else {
                    static_assert(false, "non-exhaustive visitor");
                }
            }, message->data);
        });

        if (demoted) {
            voters_.clear();
            voted_for_ = -1;
            send_disarm_timers();
            continue;
        }

        if (state_ == NodeState::Leader) continue;

        last_leader_contact = leader_contact
            ? std::chrono::steady_clock::now() // reset only if a leader message came in
            : last_leader_contact;

        // poll the election timer
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(last_leader_contact - now);

        if (duration < election_timeout_) continue;

        #ifdef DEBUG
        std::cout << "found election timeout...\n";
        #endif

        // start election
        if (state_ == NodeState::Candidate) continue;
        state_ = NodeState::Candidate;
        ++current_term_;
        voted_for_ = MY_ID;
        last_leader_contact = std::chrono::steady_clock::now();

        for (NodeID id : node_ids_) {
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
