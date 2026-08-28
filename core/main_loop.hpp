#pragma once
#include "./node.hpp"
#include <algorithm>
#include <cstdio>
#ifdef DEBUG
#include <iostream>
#include <chrono>
#endif

inline void Node::MainLoop() {
    #ifdef DEBUG
    std::cout << "starting node loop (main thread)\n";
    #endif
    while (running_) {
        // TODO: notify client that entries were committed
        if (last_applied_idx_ != commit_index_) {
            #ifdef DEBUG
            std::cout << "applying log entries from last applied index = " << last_applied_idx_ + 1 << " up to and including commit index = " << commit_index_ << "\n";
            print_cluster();
            #endif
            for (size_t i = last_applied_idx_ + 1; i <= commit_index_; ++i) {
                #ifdef DEBUG
                std::cout << "applying entry at logical index " << i << "\n";
                #endif
                apply_entry(sm_fp_, log_[i - base_logical_idx_]);
            }
            // last_applied_term_ = log_.empty() ? last_applied_term_ : log_[commit_index_ - (last_applied_idx_ + 1)].term;
            last_applied_idx_ = commit_index_;
            last_applied_term_ = log_.empty() ? base_term_ : log_[commit_index_ - base_logical_idx_].term;
            #ifdef DEBUG
            std::cout << "last_applied_idx_ set to " << last_applied_idx_ << "\n";
            std::cout << "last_applied_term_ set to " << last_applied_term_ << "\n";
            #endif
        }

        // check the reply inbox for new replies that have arrived
        bool leader_contact{false};
        inbox_->DrainAll([this, &leader_contact](NodeMessage&& message) {
            #ifdef DEBUG
            std::cout << "draining node inbox...\n";
            #endif
            std::optional<std::string> err = std::visit([this, &message, &leader_contact](auto&& payload) -> std::optional<std::string> {
                using T = std::decay_t<decltype(payload)>;

                /* Handlers */

                if constexpr (std::is_same_v<T, AppendEntriesReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found AE RPC from node " << payload.leader_id << "\n";
                    std::cout << "state_ = " << static_cast<int>(state_) << "\n";
                    std::cout << "payload term = " << payload.term << "\n";
                    std::cout << "current term = " << current_term_ << "\n";
                    std::cout << "entries_len = " << payload.entries_len << "\n";
                    std::cout << "prev log index = " << payload.prev_log_idx << "\n";
                    std::cout << "prev log term = " << payload.prev_log_term << "\n";
                    std::cout << "commit index = " << payload.leader_commit << "\n";
                    std::cout << "log size = " << log_.size() << "\n";

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
                    // log doesn't contain an entry at prev_log_index whose term matches prev_log_term.
                    // prev_log_idx == base_logical_idx_ - 1 references the last entry covered by the
                    // snapshot, which is validated against base_term_ instead
                    if (current_term_ > payload.term) {
                        #ifdef DEBUG
                        std::cout << "rejecting AE RPC: current term > payload term\n";
                        #endif
                        send(AppendEntriesRespPayload{
                            .entries_len = 0,
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .prev_log_idx = payload.prev_log_idx,
                            .term = current_term_,
                            .success = 0}, el);
                        return {};
                    }

                    if (payload.prev_log_idx < base_logical_idx_ - 1) {
                        #ifdef DEBUG
                        std::cout << "rejecting AE RPC: payload prev log idx < base logical idx - 1\n";
                        #endif
                        send(AppendEntriesRespPayload{
                            .entries_len = 0,
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .prev_log_idx = payload.prev_log_idx,
                            .term = current_term_,
                            .success = 0}, el);
                        return {};
                    }

                    if (payload.prev_log_idx == base_logical_idx_ - 1) {
                        if (payload.prev_log_term != base_term_) {
                            #ifdef DEBUG
                            std::cout << "rejecting AE RPC: payload prev log idx == base logical idx - 1 and payload prev log term != base term\n";
                            #endif
                            send(AppendEntriesRespPayload{
                                .entries_len = 0,
                                .client_fd = payload.fd,
                                .server_id = MY_ID,
                                .prev_log_idx = payload.prev_log_idx,
                                .term = current_term_,
                                .success = 0}, el);
                            return {};
                        }
                    }
                    else {
                        const size_t log_offset = payload.prev_log_idx - base_logical_idx_;
                        #ifdef DEBUG
                        std::cout << "log offset = " << log_offset << "\n";
                        #endif
                        if (log_offset >= log_.size() || log_[log_offset].term != payload.prev_log_term) {
                            #ifdef DEBUG
                            std::cout << "rejecting AE RPC: log offset >= log_size or log[log_offset].term != payload.prev_log_term\n";
                            #endif
                            send(AppendEntriesRespPayload{
                                .entries_len = 0,
                                .client_fd = payload.fd,
                                .server_id = MY_ID,
                                .prev_log_idx = payload.prev_log_idx,
                                .term = current_term_,
                                .success = 0}, el);
                            return {};
                        }
                    }

                    if (payload.entries_len == 0) {
                        if (current_term_ != payload.term) advance_to_term(payload.term);
                        leader_id_ = payload.leader_id;
                        leader_contact = true;

                        if (payload.leader_commit > commit_index_) {
                            commit_index_ = std::min(payload.leader_commit, static_cast<uint32_t>(payload.prev_log_idx));
                        }

                        #ifdef DEBUG
                        std::cout << "accepting AE RPC\n";
                        #endif
                        send(AppendEntriesRespPayload{
                            .entries_len = payload.entries_len,
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .prev_log_idx = payload.prev_log_idx,
                            .term = current_term_,
                            .success = 1}, el);
                        return {};
                    }

                    const size_t after_prev_offset = payload.prev_log_idx >= base_logical_idx_
                        ? payload.prev_log_idx - base_logical_idx_ + 1
                        : 0;

                    #ifdef DEBUG
                    std::cout << "after_prev_offset = " << after_prev_offset << "\n";
                    #endif
                    // if an existing entry conflicts w/ a new one (same index but
                    // different terms), delete the existing entry and all that
                    // follow it. The scan is bounded by BOTH the number of local
                    // log entries after prev_log_idx and the number of incoming
                    // entries, so neither log_ nor payload.entries is indexed OOB.
                    const size_t existing_after_prev = log_.size() - after_prev_offset;
                    const size_t scan_limit = std::min(existing_after_prev, payload.entries_len);
                    size_t i = 0;
                    bool log_truncated = false;
                    for (; i < scan_limit; ++i) {
                        const size_t log_idx = after_prev_offset + i;
                        if (log_[log_idx].term != payload.entries[i].term) {
                            // conflict: truncate the local log from this index on.
                            log_.erase(log_.begin() + log_idx, log_.end());
                            log_truncated = true;
                            break;
                        }
                    }

                    // append any entries not already in the log
                    size_t start = log_.size();
                    log_.resize(log_.size() + payload.entries_len - i);
                    std::memcpy(&log_[start], payload.entries + i, (payload.entries_len - i) * sizeof(LogEntry));

                    if (log_truncated) {
                        ::freopen(LOG_FILE_PATH, "w+", log_fp_);
                        ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
                        ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
                        ::fwrite(log_.data(), sizeof(LogEntry), log_.size(), log_fp_);
                    }
                    else {
                        ::fseek(log_fp_, 0, SEEK_END);
                        ::fwrite(&log_[start], sizeof(LogEntry), payload.entries_len - i, log_fp_);
                    }

                    if (payload.leader_commit > commit_index_) {
                        commit_index_ = std::min(payload.leader_commit, static_cast<uint32_t>(payload.prev_log_idx + payload.entries_len));
                    }

                    if (current_term_ != payload.term) advance_to_term(payload.term);
                    leader_id_ = payload.leader_id;
                    leader_contact = true;

                    #ifdef DEBUG
                    std::cout << "accepting AE RPC\n";
                    #endif
                    send(AppendEntriesRespPayload{
                        .entries_len = payload.entries_len,
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .prev_log_idx = payload.prev_log_idx,
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
                    print_cluster();

                    std::cout << "Current state = " << static_cast<int>(state_) << "\n";

                    std::cout << "Payload term = " << payload.term << ", this node's term = " << current_term_ << "\n";
                    std::cout << "Payload last log term = " << payload.last_log_term << "\n";
                    std::cout << "Payload last log idx = " << payload.last_log_idx << "\n";
                    std::cout << "This node's voted_for = " << voted_for_ << "\n";
                    std::cout << "This node's voters = ";
                    for (auto v : voters_) {
                        std::cout << v << ", ";
                    }
                    std::cout << "\n";

                    #endif

                    if (payload.term > current_term_) {
                        advance_to_term(payload.term);
                        leader_id_ = payload.candidate_id;
                        leader_contact = true;
                    }

                    if (payload.term < current_term_) {
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

                    const uint32_t last_log_idx = static_cast<uint32_t>(log_.size() - 1) + base_logical_idx_; // logical index
                    const uint32_t last_log_term = log_.empty() ? base_term_ : log_.back().term;
                    #ifdef DEBUG
                    std::cout << "last_log_idx = " << last_log_idx << "\n";
                    std::cout << "last_log_term = " << last_log_term << "\n";
                    #endif
                    if ((voted_for_ == -1 || voted_for_ == payload.candidate_id)
                    &&  (payload.last_log_term > last_log_term
                    || (payload.last_log_term == last_log_term
                        && payload.last_log_idx >= last_log_idx)))
                    {
                        voted_for_ = payload.candidate_id;
                        write_voted_for();
                        send(RequestVoteRespPayload{
                            .client_fd = payload.fd,
                            .server_id = MY_ID,
                            .term = current_term_,
                            .vote_granted = 1}, el);
                        #ifdef DEBUG
                        std::cout << "voting for node " << payload.candidate_id << "\n";
                        #endif
                        demote();
                        leader_contact = true; // to reset the election timer
                        return {};
                    }

                    #ifdef DEBUG
                    std::cout << "(default) rejecting RV from node " << payload.candidate_id << "\n";
                    #endif
                    send(RequestVoteRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_,
                        .vote_granted = 0}, el);
                    return {};

                }

                else if constexpr (std::is_same_v<T, InstallSnapshotReqPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS RPC from node " << payload.leader_id << "\n";
                    std::cout << "payload.term = " << payload.term << "\n";
                    std::cout << "payload.last_included_idx = " << payload.last_included_idx << "\n";
                    std::cout << "payload.last_included_term = " << payload.last_included_term << "\n";
                    std::cout << "payload.offset = " << payload.offset << "\n";
                    std::cout << "payload.leader_id = " << payload.leader_id << "\n";
                    std::cout << "payload.done = " << static_cast<int>(payload.done) << "\n";
                    #endif
                    auto& el = loops_[payload.leader_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.leader_id, payload.fd, el);

                    if (payload.term > current_term_) {
                        advance_to_term(payload.term);
                        leader_id_ = payload.leader_id;
                        leader_contact = true;
                    }

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

                    #ifdef DEBUG
                    std::cout << "accepting snapshot chunk\n";
                    #endif
                    send(InstallSnapshotRespPayload{
                        .client_fd = payload.fd,
                        .server_id = MY_ID,
                        .term = current_term_}, el);

                    // for the first snapshot chunk, the cluster data is given at the start, after the last_included_idx and last_included_term
                    if (payload.offset == 0) {
                        if (snapshot_tmp_fp_ != nullptr) {
                            ::fclose(snapshot_tmp_fp_);
                        }
                        snapshot_tmp_fp_ = ::fopen(SNAPSHOT_TMP_FILE_PATH, "w+");
                        if (snapshot_tmp_fp_ == NULL) return (
                            "error in InstallSnapshotRPC handler; failed to open snapshot tmp file\n"
                        );
                        size_t cluster_size;
                        std::memcpy(&cluster_size, payload.partial_state + snapshot_config_and_data_offset_bytes(), sizeof(cluster_size));
                        node_ids_.reset_cluster(payload.partial_state + snapshot_config_and_data_offset_bytes() + sizeof(cluster_size), cluster_size);
                        node_ids_.set_unavailable_node(MY_ID);
                    }

                    // write data into snapshot file at given offset
                    ::fwrite(payload.partial_state, payload.data_len, 1, snapshot_tmp_fp_);

                    if (payload.done == 0) return {};
                    // leader sent the last chunk

                    // commit the temp file to disk
                    ::fflush(snapshot_tmp_fp_);
                    ::fsync(fileno(snapshot_tmp_fp_));
                    ::fclose(snapshot_tmp_fp_);
                    snapshot_tmp_fp_ = nullptr;
                    ::rename(SNAPSHOT_TMP_FILE_PATH, SNAPSHOT_FILE_PATH);

                    snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");

                    if (payload.last_included_idx > last_applied_idx_) {
                        const size_t offset = payload.last_included_idx - base_logical_idx_;
                        if (offset < log_.size() && log_[offset].term == payload.last_included_term) {
                            // Matching entry found; discard entries up to and including last_included_idx
                            log_.erase(log_.begin(), log_.begin() + offset + 1);
                        } else {
                            // Conflict or snapshot extends beyond log; discard all entries
                            log_.clear();
                        }

                        #ifdef DEBUG
                        std::cout << "overwriting log file w/ current term, voted for, and log entries\n";
                        #endif
                        ::freopen(LOG_FILE_PATH, "w+", log_fp_);
                        ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
                        ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
                        ::fwrite(log_.data(), sizeof(LogEntry), log_.size(), log_fp_);
                    }

                    __off64_t snapshot_header = static_cast<__off64_t>(snapshot_header_bytes());
                    __off64_t sm_header = static_cast<__off64_t>(sm_header_bytes());

                    // add the installed snapshot to the state machine file
                    struct stat snapshot_stat;
                    if (stat(SNAPSHOT_FILE_PATH, &snapshot_stat) != 0
                        || snapshot_stat.st_size < snapshot_header) {
                        return (std::format(
                            "error in InstallSnapshotRPC handler; couldn't restore state machine from snapshot {}: bad snapshot size\n",
                            SNAPSHOT_FILE_PATH
                        ));
                    }

                    if (sm_fp_ != nullptr) {
                        ::fclose(sm_fp_);
                    }
                    sm_fp_ = ::fopen(STATE_MACHINE_FILE_PATH, "w+");
                    if (sm_fp_ == NULL) {
                        return (std::format(
                            "error in InstallSnapshotRPC handler; failed to open state machine file {}\n",
                            STATE_MACHINE_FILE_PATH
                        ));
                    }

                    __off64_t zero{0};
                    __off64_t snapshot_off = static_cast<__off64_t>(snapshot_config_and_data_offset_bytes());
                    ssize_t n = copy_file_range(fileno(snapshot_fp_), &snapshot_off,
                                                fileno(sm_fp_), &zero,
                                                snapshot_stat.st_size - snapshot_config_and_data_offset_bytes(), 0);
                    if (n < 0) {
                        ::fclose(sm_fp_);
                        sm_fp_ = nullptr;
                        return "error in InstallSnapshotRPC handler; failed to copy snapshot state into the state machine\n";
                    }

                    ::fflush(sm_fp_);
                    ::fsync(fileno(sm_fp_));

                    commit_index_ = payload.last_included_idx;
                    last_applied_idx_ = payload.last_included_idx;
                    last_applied_term_ = payload.last_included_term;
                    base_logical_idx_ = payload.last_included_idx + 1;
                    base_term_ = payload.last_included_term;
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
                        advance_to_term(payload.term);
                        leader_id_ = payload.server_id;
                        leader_contact = true;
                        return {};
                    }

                    if (payload.success == 1) {
                        next_indexes_[payload.server_id] = payload.prev_log_idx + 1 + payload.entries_len;
                        match_indexes_[payload.server_id] = payload.prev_log_idx + payload.entries_len;

                        commit_entries_if_available();
                        return {};
                    }

                    // const size_t prev_log_idx_offset = payload.prev_log_idx < base_logical_idx_ ? 0 : payload.prev_log_idx - base_logical_idx_;
                    // if (payload.prev_log_idx != base_logical_idx_ - 1 && prev_log_idx_offset >= log_.size()) {
                    //     return (std::format(
                    //         "Failed to process AE reply: prev_log_idx offset {} out of bounds (log size = {})",
                    //         prev_log_idx_offset, log_.size()
                    //     ));
                    // }

                    next_indexes_[payload.server_id] = payload.prev_log_idx;
                    if (next_indexes_[payload.server_id] < 1) {
                        return (std::format(
                            "Failed to retry AE RPC: next_index {} for server id {} already at the snapshot boundary",
                            next_indexes_[payload.server_id], payload.server_id
                        ));
                    }
                    auto& el = loops_[payload.server_id & (EVENT_LOOP_THREADS - 1)];

                    if (payload.prev_log_idx < base_logical_idx_) {
                        installing_snapshot_.set(payload.server_id);
                        std::optional<std::string> send_is_err = send_install_snapshot(el, payload.server_id);
                        if (send_is_err) {
                            return (std::format(
                                "error retrying IS RPC:\n{}\n",
                                send_is_err.value()
                            ));
                        }
                        return {};
                    }
                    return send_append_entries(next_indexes_[payload.server_id], el, payload.server_id);

                }

                else if constexpr (std::is_same_v<T, RequestVoteRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found RV reply from node " << payload.server_id << "\n";
                    print_cluster();

                    std::cout << "Current state = " << static_cast<int>(state_) << "\n";

                    std::cout << "Payload term = " << payload.term << ", this node's term = " << current_term_ << "\n";
                    std::cout << "vote granted = " << static_cast<int>(payload.vote_granted) << "\n";
                    std::cout << "This node's voted_for = " << voted_for_ << "\n";
                    std::cout << "This node's voters = ";
                    for (auto v : voters_) {
                        std::cout << v << ", ";
                    }
                    std::cout << "\n";
                    #endif

                    if (payload.term > current_term_) {
                        advance_to_term(payload.term);
                        leader_id_ = payload.server_id;
                        leader_contact = true;
                    }

                    if (payload.vote_granted == 0
                        || state_ != NodeState::Candidate
                        || payload.term < current_term_
                        || !voters_.insert(payload.server_id).second
                    ) {
                        #ifdef DEBUG
                        std::cout << "this node is no longer a candidate, or node " << payload.server_id << " has already voted for this node, or payload term is less than this node's current term; skipping\n";
                        #endif
                        return {};
                    }

                    // become leader if quorum of votes achieved
                    if (voters_.size() > node_ids_.num_in_cluster / 2) {
                        become_leader();
                    }
                    return {};
                }

                else if constexpr (std::is_same_v<T, InstallSnapshotRespPayload>) {
                    #ifdef DEBUG
                    std::cout << "found IS reply from node " << payload.server_id << "\n";
                    std::cout << "payload.term = " << payload.term << "\n";
                    #endif

                    if (payload.term > current_term_) {
                        advance_to_term(payload.term);
                    }

                    struct stat snapshot_stat;
                    if (stat(SNAPSHOT_FILE_PATH, &snapshot_stat) != 0) {
                        return "failed to send snapshot chunk; couldn't get snapshot file size\n";
                    }
                    if (snapshot_stat.st_size < static_cast<off_t>(snapshot_header_bytes())) {
                        return "failed to send snapshot chunk; snapshot file smaller than its header\n";
                    }

                    if (++chunks_sent_[payload.server_id] * SNAPSHOT_CHUNK_SIZE <= snapshot_stat.st_size) {
                        auto& el = loops_[payload.server_id & (EVENT_LOOP_THREADS - 1)];
                        send_install_snapshot(el, payload.server_id);
                        return {};
                    }

                    // done sending
                    chunks_sent_[payload.server_id] = 0;
                    next_indexes_[payload.server_id] = base_logical_idx_;
                    match_indexes_[payload.server_id] = base_logical_idx_ - 1;
                    installing_snapshot_.unset(payload.server_id);
                }

                // heartbeats are sent per follower, not all at once.
                else if constexpr (std::is_same_v<T, HeartbeatTimeout>) {
                    if (state_ != NodeState::Leader) return {}; // in case this node was demoted in the interim
                    #ifdef DEBUG
                    std::cout << "found heartbeat timeout for node " << payload.source_id << "; sending heartbeat...\n";
                    std::cout << "next index = " << next_indexes_[payload.source_id] << "\n";
                    std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
                    std::cout << "base_logical_idx_ = " << base_logical_idx_ << "\n";
                    #endif
                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    const int32_t next_idx = next_indexes_[payload.source_id];
                    if (next_idx == 0) {
                        return (std::format(
                            "Failed to process HeartbeatTimeout: next_index 0 for node id {} cannot derive prev_log_idx",
                            payload.source_id
                        ));
                    }

                    // if last log index >= this follower's nextIndex,
                    // then send AE RPC w/ log entries starting at nextIndex. Otherwise, send term w/ no entries

                    if (next_idx < base_logical_idx_) {
                        installing_snapshot_.set(payload.source_id);

                        std::optional<std::string> send_is_err = send_install_snapshot(el, payload.source_id);
                        if (send_is_err) {
                            return (std::format(
                                "error retrying IS RPC:\n{}\n",
                                send_is_err.value()
                            ));
                        }
                        return {};
                    }
                    std::optional<std::string> send_ae_err = send_append_entries(next_idx, el, payload.source_id);
                    if (send_ae_err) {
                        return (std::format(
                            "error retrying AE RPC:\n{}\n",
                            send_ae_err.value()
                        ));
                    }
                }

                else if constexpr (std::is_same_v<T, DropPeerMsg>) {
                    #ifdef DEBUG
                    std::cout << "Received drop peer message - dropping peer " << payload.source_id << "\n";
                    #endif
                    if (payload.source_id < node_ids_.bits()) {
                        node_ids_.set_unavailable_node(payload.source_id);
                    }
                    next_indexes_[payload.source_id] = -1;
                    //match_indexes_[payload.source_id] = -1;
                    chunks_sent_[payload.source_id] = -1;

                    voters_.erase(payload.source_id);
                    if (voted_for_ == payload.source_id) {
                        voted_for_ = -1;
                        write_voted_for();
                    }

                    if (leader_id_ == payload.source_id) {
                        leader_id_ = -1;
                    }
                }

                else if constexpr (std::is_same_v<T, ForwardLeaderMsg>) {
                    #ifdef DEBUG
                    std::cout << "found ForwardLeader message\n";
                    for (int i = 0; i < payload.entries_len; ++i) {
                        for (int j = 0; j < CMD_SIZE; ++j) {
                            std::cout << static_cast<int>(payload.entries[i][j]) << ", ";
                        }
                        std::cout << "\n";

                    }
                    #endif

                    if (payload.term != current_term_) {
                        #ifdef DEBUG
                        std::cout << "forwarded request has a stale term = " << payload.term << "\n";
                        #endif
                        return {};
                    }
                    auto& el = loops_[payload.sender_id & (EVENT_LOOP_THREADS - 1)];
                    add_peer_if_not_exists(payload.sender_id, payload.fd, el);
                    append_commands(payload.entries, payload.entries_len);
                }

                else if constexpr (std::is_same_v<T, AETimeout>) {
                    // Stale AE retry timer after demotion; only a leader retries AEs.
                    if (state_ != NodeState::Leader || !node_ids_.is_available(payload.source_id)) return {};
                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    const int32_t next_idx = next_indexes_[payload.source_id];
                    if (next_idx == 0) {
                        return (std::format(
                            "Failed to process HeartbeatTimeout: next_index 0 for node id {} cannot derive prev_log_idx",
                            payload.source_id
                        ));
                    }

                    if (next_idx < base_logical_idx_) {
                        installing_snapshot_.set(payload.source_id);
                        std::optional<std::string> send_is_err = send_install_snapshot(el, payload.source_id);
                        if (send_is_err) {
                            return (std::format(
                                "error retrying IS RPC:\n{}\n",
                                send_is_err.value()
                            ));
                        }
                        return {};
                    }

                    std::optional<std::string> send_ae_err = send_append_entries(next_idx, el, payload.source_id);
                    if (send_ae_err) {
                        return (std::format(
                            "error retrying AE RPC:\n{}\n",
                            send_ae_err.value()
                        ));
                    }
                }

                else if constexpr (std::is_same_v<T, RVTimeout>) {
                    // Only a candidate still seeking votes retries; guards
                    // against a stale RV timer firing after winning/demotion.
                    if (state_ != NodeState::Candidate || !node_ids_.is_available(payload.source_id)) return {};
                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    // retry
                    auto p = RequestVoteReqPayload{
                        .dest_id = payload.source_id,
                        .term = current_term_,
                        .candidate_id = MY_ID,
                        .last_log_idx = static_cast<uint32_t>(log_.size() - 1) + base_logical_idx_,
                        .last_log_term = log_.empty() ? base_term_ : log_.back().term,
                    };
                    send(std::move(p), el);
                }

                else if constexpr (std::is_same_v<T, ISTimeout>) {
                    // retry; only a leader (still installing this snapshot) retries
                    if (state_ != NodeState::Leader || !installing_snapshot_.is_set(payload.source_id)) return {};

                    auto& el = loops_[payload.source_id & (EVENT_LOOP_THREADS - 1)];
                    std::optional<std::string> send_is_err = send_install_snapshot(el, payload.source_id);
                    if (send_is_err) {
                        return (std::format(
                            "error retrying IS RPC:\n{}\n",
                            send_is_err.value()
                        ));
                    }
                }

                else if constexpr (std::is_same_v<T, StopNodeMsg>) {
                    running_ = false;
                }

                else {
                    static_assert(false, "non-exhaustive visitor");
                }
                return {};
            }, message);
            #ifdef DEBUG
            if (err) std::cout << "inbox handler error: " << err.value() << "\n";
            #else
            (void)err;
            #endif
        });

        // Periodic flush of log, snapshot, and state machine files
        auto flush_now = std::chrono::steady_clock::now();
        if (flush_now - last_flush_ >= FLUSH_INTERVAL) {
            flush_files();
        }

        // only compact entries that have been applied
        if (log_.size() >= LOG_COMPACT_THRESHOLD
            && last_applied_idx_ >= base_logical_idx_) {
            #ifdef DEBUG
            std::cout << "log size reached compact threshold; compacting...\n";
            std::cout << "log_.size() = " << log_.size() << "\n";
            std::cout << "last_applied_idx_ = " << last_applied_idx_ << "\n";
            std::cout << "base_logical_idx_ = " << base_logical_idx_ << "\n";
            std::cout << "commit_index_ = " << commit_index_ << "\n";
            #endif
            const size_t num_applied = last_applied_idx_ - base_logical_idx_ + 1;
            #ifdef DEBUG
            std::cout << "num_applied = " << num_applied << "\n";
            #endif
            base_term_ = log_[num_applied - 1].term;
            #ifdef DEBUG
            std::cout << "base_term_ = " << base_term_ << "\n";
            #endif
            log_.erase(log_.begin(), log_.begin() + num_applied);
            base_logical_idx_ = last_applied_idx_ + 1;
            #ifdef DEBUG
            std::cout << "base_logical_idx_ = " << base_logical_idx_ << "\n";
            std::cout << "log_.size() after compaction = " << log_.size() << "\n";
            #endif

            ::freopen(LOG_FILE_PATH, "w+", log_fp_); // clears the file and sets the file position to the beginning
            ::fwrite(&current_term_, sizeof(current_term_), 1, log_fp_);
            ::fwrite(&voted_for_, sizeof(voted_for_), 1, log_fp_);
            // preserve any unapplied entries so they can be replayed after a restart
            if (!log_.empty()) {
                ::fwrite(log_.data(), sizeof(LogEntry), log_.size(), log_fp_);
            }

            // create the snapshot and write to disk
            snapshot_tmp_fp_ = ::fopen(SNAPSHOT_TMP_FILE_PATH, "w+");
            size_t cluster_size_bytes = node_ids_.bytes();
            ::fwrite(&last_applied_idx_, sizeof(last_applied_idx_), 1, snapshot_tmp_fp_);
            ::fwrite(&last_applied_term_, sizeof(last_applied_term_), 1, snapshot_tmp_fp_);
            ::fwrite(&cluster_size_bytes, sizeof(cluster_size_bytes), 1, snapshot_tmp_fp_);
            ::fwrite(node_ids_.cluster.data(), cluster_size_bytes, 1, snapshot_tmp_fp_);
            create_snapshot(snapshot_tmp_fp_, sm_fp_); // caller-defined

            // commit the temp file to disk
            ::fflush(snapshot_tmp_fp_);
            ::fsync(fileno(snapshot_tmp_fp_));

            ::fclose(snapshot_tmp_fp_);
            snapshot_tmp_fp_ = nullptr;
            ::rename(SNAPSHOT_TMP_FILE_PATH, SNAPSHOT_FILE_PATH);
            snapshot_fp_ = ::fopen(SNAPSHOT_FILE_PATH, "r+");
        }

        if (state_ == NodeState::Leader) continue;

        if (leader_contact) {
            last_leader_contact_ = std::chrono::steady_clock::now();
            demote();
        }

        // poll the election timer
        auto now = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_leader_contact_);

        if (duration < election_timeout_) continue;

        // start election
        #ifdef DEBUG
        std::cout << "election timeout; starting election...\n";
        #endif

        state_ = NodeState::Candidate;
        // set timeout to a new random value
        election_timeout_ = std::chrono::milliseconds(distrib_(rand_gen_));
        #ifdef DEBUG
        std::cout << "set election timeout to " << election_timeout_ << "\n";
        #endif

        ++current_term_;
        voted_for_ = MY_ID;
        write_current_term();
        write_voted_for();
        voters_.clear();
        voters_.insert(MY_ID);
        last_leader_contact_ = std::chrono::steady_clock::now();

        // A node always votes for itself. If that single vote is already a
        // majority (e.g. a single-node cluster with no peers), win the
        // election immediately rather than waiting for RequestVote replies
        // that will never come.
        if (voters_.size() > (node_ids_.num_in_cluster / 2)) {
            become_leader();
            continue;
        }
        request_votes();
    }
}
