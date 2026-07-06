#include "./node.hpp"
#include <algorithm>
#include <cstdio>
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
                    const size_t log_offset = payload.prev_log_idx - last_applied_idx_;

                    if (current_term_ > payload.term
                    || log_offset >= log_.size()
                    || log_[log_offset].term != payload.prev_log_term
                    ) {
                        send(AppendEntriesRespPayload{
                            .entries_len = 0,
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
                    const size_t existing_after_prev = log_.size() - log_offset - 1;
                    const size_t scan_limit = std::min(existing_after_prev, payload.entries.size());
                    size_t i = 1;
                    for (; i < scan_limit; ++i) {
                        const size_t log_idx = log_offset + i + 1;
                        if (log_[log_idx].term != payload.entries[i].term) {
                            // conflict: truncate the local log from this index on.
                            log_.erase(log_.begin() + log_idx, log_.end());
                            break;
                        }
                    }

                    // append any entries not already in the log
                    log_.reserve(payload.entries.size());
                    for (auto it = payload.entries.begin() + i; it < payload.entries.end(); ++it) {
                        log_.emplace_back(it->data_, CMD_SIZE, it->term);
                    }

                    if (payload.leader_commit > commit_index_) {
                        commit_index_ = std::min(payload.leader_commit, static_cast<uint32_t>(payload.prev_log_idx + payload.entries.size()));
                    }

                    for (size_t i = last_applied_idx_ + 1; i <= commit_index_; ++i) {
                        apply_entry(log_[i - last_applied_idx_]);
                    }
                    last_applied_idx_ = commit_index_;

                    current_term_ = payload.term;
                    if (state_ != NodeState::Follower) demote();
                    leader_id = payload.leader_id;
                    leader_contact = true;

                    send(AppendEntriesRespPayload{
                        .entries_len = payload.entries.size(),
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_,
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

                    leader_id = payload.candidate_id;
                    leader_contact = true;
                    const uint32_t last_log_term = log_.back().term;
                    const uint32_t last_log_idx = static_cast<uint32_t>(log_.size() - 1) + last_applied_idx_; // logical index
                    if (payload.last_log_term > last_log_term
                    || (payload.last_log_term == last_log_term
                        && payload.last_log_idx >= last_log_idx))
                    {
                        voted_for_ = payload.candidate_id;
                        send(RequestVoteRespPayload{
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .term = current_term_,
                            .vote_granted = 1}, el);
                        #ifdef DEBUG
                        std::cout << "voting for node " << payload.candidate_id << "\n";
                        #endif
                    }

                    return {};
                }

                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC from node " << payload.leader_id << "\n";
                    #endif
                    auto& el = loops_[payload.leader_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.leader_id, payload.fd, el);

                    if (payload.term < current_term_ || payload.last_included_idx < last_applied_idx_) {
                        #ifdef DEBUG
                        if (payload.term < current_term_) {
                            std::cout << "payload.term = " << payload.term << ", current term = " << current_term_ << "; rejecting IS RPC from node " << payload.leader_id << "\n";
                        }
                        else {
                            std::cout << "payload last included index = " << payload.last_included_idx << ", this last included index = " << last_applied_idx_ << "; rejecting IS RPC from node " << payload.leader_id << "\n";
                        }
                        #endif
                        return {};
                    }

                    else if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demote();
                    }

                    send(InstallSnapshotRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_}, el);

                    leader_id = payload.leader_id;
                    leader_contact = true;

                    // TODO: include cluster state in snapshots; replace node_ids_ vector w/ std::bit_set

                    // write data into snapshot file at given offset
                    if (payload.offset == 0) {
                        snapshot_tmp_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "w+");
                        if (snapshot_tmp_fp_ == NULL) return Unexpected(
                            "error in InstallSnapshotRPC handler; failed to open snapshot tmp file\n"
                        );
                    }
                    else ::fseek(snapshot_tmp_fp_, payload.offset, SEEK_SET); // is this needed?

                    ::fwrite(&payload.partial_state, SNAPSHOT_CHUNK_SIZE, 1, snapshot_tmp_fp_);

                    if (payload.done == 0) return {};
                    // leader sent the last chunk
                    ::fwrite(&payload.last_included_idx, sizeof(uint32_t), 2, snapshot_tmp_fp_); // add the last_included_idx and last_included_term at the end

                    // commit the temp file to disk
                    ::fflush(snapshot_tmp_fp_);
                    ::fsync(fileno(snapshot_tmp_fp_));

                    // atomically replace the snapshot file with the temporary
                    ::rename(SNAPSHOT_TMP_FILE_PATH, SNAPSHOT_FILE_PATH);

                    if (payload.last_included_idx - last_applied_idx_ > 0
                        && payload.last_included_idx - last_applied_idx_ < log_.size()) {
                            // if the last included entry of the snapshot matches with that of this node's log, discard all entries up to last_included_idx
                            if (log_[payload.last_included_idx - last_applied_idx_].term == payload.last_included_term) {
                                log_.erase(log_.begin() + 1, log_.begin() + (payload.last_included_idx - last_applied_idx_) + 1);
                                ::freopen(LOG_FILE_PATH, "w+", log_fp_);
                                ::fseek(log_fp_, 1, SEEK_SET);
                                // copy the log to the file
                                ::fwrite(log_.data(), sizeof(LogEntry), log_.size(), log_fp_);
                                ::fflush(log_fp_);
                                ::fsync(fileno(log_fp_));
                            }
                            // otherwise, there is a conflict, so this node's log must be cleared entirely.
                            else {
                                log_.clear();
                                ::freopen(LOG_FILE_PATH, "w+", log_fp_);
                            }
                    }
                    last_applied_idx_ = payload.last_included_idx;
                    last_applied_term_ = payload.last_included_term;
                    return {};
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

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demote();
                        return {};
                    }

                    const int32_t stored_next = next_indexes_[payload.server_id];
                    if (stored_next < 1) {
                        return UnexpectedF(std::format(
                            "Failed to process AE reply: next_index {} for server id {} too small to derive prev_log_idx",
                            stored_next, payload.server_id
                        ));
                    }
                    const uint32_t prev_log_idx = stored_next - 1;
                    const size_t prev_log_idx_offset = prev_log_idx - last_applied_idx_;
                    if (prev_log_idx_offset >= log_.size()) {
                        return UnexpectedF(std::format(
                            "Failed to process AE reply: prev_log_idx offset {} out of bounds (log size = {})",
                            prev_log_idx_offset, log_.size()
                        ));
                    }
                    const uint32_t prev_log_term = log_[prev_log_idx_offset].term;

                    /*
                     on fail:
                     decrement nextIndex and retry
                    */
                    next_indexes_[payload.server_id] = prev_log_idx;
                    if (payload.success == 0) {
                        auto entries_to_append = std::span<LogEntry>(log_.data() + prev_log_idx_offset, log_.size() - prev_log_idx_offset);

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
                    next_indexes_[payload.server_id] += payload.entries_len;
                    match_indexes_[payload.server_id] = prev_log_idx + payload.entries_len;

                    /*
                     if there exists an N such that
                     N > commit_index,
                     a majority of matchIndex[N] >= N,
                     and log[N].term == currentTerm,
                     set commitIndex = N

                     i.e. if a majority of servers have a matchIndex greater than or equal to index N,
                     and the entry at N is in the current term,
                     then set commitIndex to N
                     --> entries <= commitIndex become committed.
                     */
                    uint32_t old_commit_idx = commit_index_;
                    uint32_t new_commit_idx = compute_new_commit_idx();
                    if (old_commit_idx == new_commit_idx) return {};
                    commit_index_ = new_commit_idx;

                    for (size_t i = last_applied_idx_ + 1; i <= commit_index_; ++i) {
                        apply_entry(log_[i - last_applied_idx_]);
                    }
                    last_applied_idx_ = commit_index_;

                    // auto it = log_.begin() + (old_commit_idx - (snapshot.last_applied_idx + 1));
                    // ::fwrite(&*it, sizeof(LogEntry), new_commit_idx - old_commit_idx, log_fp);
                    // // to ensure crash-safety
                    // ::fflush(log_fp);
                    // ::fsync(fileno(log_fp));

                    // TODO: notify client that this entry/entries were committed

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
                        || !voters_.insert(payload.server_id).second
                        || payload.term < current_term_
                    ) {
                        #ifdef DEBUG
                        std::cout << "this node is no longer a candidate, or node " << payload.server_id << " has already voted for this node, or payload term is less than this node's current term; skipping\n";
                        #endif
                        return {};
                    }

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demote();
                    }

                    if (payload.vote_granted == 0) { return {}; }

                    // become leader if quorum of votes achieved
                    if (voters_.size() > (node_ids_.size() + 1) / 2) {
                        become_leader();
                    }
                    return {};
                }

                // TODO: retry sending a chunk if the leader doesn't get a reply
                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS reply from node " << payload.server_id << "\n";
                    #endif

                    if (payload.term > current_term_) {
                        current_term_ = payload.term;
                        demote();
                    }

                    if (++chunks_sent[payload.server_id] * SNAPSHOT_CHUNK_SIZE < SM_STATE_SIZE) {
                        auto& el = loops_[payload.server_id & (EVENT_LOOP_THREADS - 1)];
                        InstallSnapshotReqPayload p = InstallSnapshotReqPayload{
                            .last_included_idx = last_applied_idx_ss_,
                            .last_included_term = last_applied_term_ss_,
                            .offset = chunks_sent[payload.server_id] * SNAPSHOT_CHUNK_SIZE,
                            .dest_id = payload.server_id,
                            .leader_id = MY_ID,
                            .done = (chunks_sent[payload.server_id] + 1) * SNAPSHOT_CHUNK_SIZE >= SM_STATE_SIZE,
                        };
                        ::fseek(snapshot_fp_, chunks_sent[payload.server_id] * SNAPSHOT_CHUNK_SIZE, SEEK_SET);
                        if (::fread(p.partial_state, SNAPSHOT_CHUNK_SIZE, 1, snapshot_fp_) < 1) {
                            return UnexpectedF(std::format(
                                "failed to send InstallSnapshot request to node {} - couldn't read from snapshot file at offset {}",
                                payload.server_id, chunks_sent[payload.server_id] * SNAPSHOT_CHUNK_SIZE
                            ));
                        }
                        send(std::move(p), el);
                        return {};
                    };

                    chunks_sent[payload.server_id] = 0;
                    next_indexes_[payload.server_id] = last_applied_idx_ + 1;
                    match_indexes_[payload.server_id] = last_applied_idx_;
                    installing_snapshot_ = false;
                }

                // heartbeats are sent per follower, not all at once.
                else if constexpr (std::is_same_v<T, HeartbeatTimeout>) {
                    #ifdef DEBUG
                    std::cout << "found heartbeat timeout for node " << payload.source_id << "; sending heartbeat...\n";
                    #endif
                    // if last log index >= this follower's nextIndex,
                    // then send AE RPC w/ log entries starting at nextIndex. Otherwise, send term w/ no entries
                    const int32_t next_idx = next_indexes_[payload.source_id];
                    if (next_idx == 0) {
                        return UnexpectedF(std::format(
                            "Failed to process HeartbeatTimeout: next_index 0 for node id {} cannot derive prev_log_idx",
                            payload.source_id
                        ));
                    }

                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    if (next_idx < last_applied_idx_) {
                        installing_snapshot_ = true;
                        last_applied_idx_ss_ = last_applied_idx_;
                        last_applied_term_ss_ = last_applied_term_;

                        InstallSnapshotReqPayload p = InstallSnapshotReqPayload{
                            .last_included_idx = last_applied_idx_ss_,
                            .last_included_term = last_applied_term_ss_,
                            .offset = 0,
                            .dest_id = payload.source_id,
                            .leader_id = MY_ID,
                            .done = SNAPSHOT_CHUNK_SIZE >= SM_STATE_SIZE,
                        };

                        ::rewind(snapshot_fp_);
                        if (::fread(p.partial_state, SNAPSHOT_CHUNK_SIZE, 1, snapshot_fp_) < 1) {
                            return UnexpectedF(std::format(
                                "failed to send InstallSnapshot request to node {} - couldn't read from snapshot file at offset 0",
                                payload.source_id
                            ));
                        }
                        send(std::move(p), el);
                        return {};
                    }

                    // Only send entries when the log actually has some at/after
                    // next_idx. log_.size()-1 >= next_idx is restated as
                    // next_idx < log_.size() to avoid uint underflow on size 0.
                    const uint32_t prev_log_idx = next_idx - 1;
                    const size_t prev_log_idx_offset = prev_log_idx - last_applied_idx_;
                    const uint32_t prev_log_term = log_[prev_log_idx_offset].term;
                    const size_t next_idx_offset = next_idx - last_applied_idx_;
                    auto s = next_idx_offset < log_.size()
                    ? std::span<LogEntry>(log_.begin() + next_idx_offset, log_.end())
                    : std::span<LogEntry>{};

                    #ifdef DEBUG
                    std::cout << "sending " << s.size() << " entries\n";
                    #endif

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

                else if constexpr (std::is_same_v<T, DropPeerMsg>) {
                    #ifdef DEBUG
                    std::cout << "Received drop peer message - dropping peer " << payload.source_id << "\n";
                    #endif
                    auto it = std::find(node_ids_.begin(), node_ids_.end(), payload.source_id);
                    node_ids_.erase(it);
                    next_indexes_[payload.source_id] = -1;
                    match_indexes_[payload.source_id] = -1;
                    chunks_sent[payload.source_id] = -1;

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

        if (log_.size() >= LOG_COMPACT_THRESHOLD) {
            // TODO: if this is too slow, could try running it in a separate thread
            // discard log entries
            log_.erase(log_.begin() + 1, log_.end());

            ::freopen(LOG_FILE_PATH, "w+", log_fp_); // clears the file and sets the file position to the beginning
            ::fflush(log_fp_);
            ::fsync(fileno(log_fp_));

            // create the snapshot and write to disk
            create_snapshot(snapshot_fp_, sm_fp_); // caller-defined
        }

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

        // start election
        if (state_ == NodeState::Candidate) continue;
        #ifdef DEBUG
        std::cout << "election timeout; starting election...\n";
        #endif

        state_ = NodeState::Candidate;
        ++current_term_;
        voted_for_ = MY_ID;
        voters_.insert(MY_ID);
        last_leader_contact_ = std::chrono::steady_clock::now();

        // A node always votes for itself. If that single vote is already a
        // majority (e.g. a single-node cluster with no peers), win the
        // election immediately rather than waiting for RequestVote replies
        // that will never come.
        if (voters_.size() > (node_ids_.size() + 1) / 2) {
            become_leader();
            continue;
        }
        request_votes();
    }
}
