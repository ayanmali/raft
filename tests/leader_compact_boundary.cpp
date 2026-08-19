// All standard/system headers must be parsed BEFORE the private/public macro
// below, otherwise libstdc++ headers pulled in after it get miscompiled.
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <format>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <random>
#include <string>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <thread>
#include <type_traits>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#define private public
#define protected public
#include "../core/node.hpp"
#undef private
#undef protected

// Regression test: a leader whose log has been fully compacted into a snapshot
// (log_.size() == 0, base_logical_idx_ == N) sends a fresh follower an AE
// heartbeat with prev_log_idx == base_logical_idx_ - 1 (the snapshot
// boundary). The follower rejects it because its log has no entry at that
// index. The AE-reply handler must NOT bail on the empty-log OOB guard in that
// boundary case; it must decrement next_index below the boundary and route the
// follower through InstallSnapshot instead.
int main() {
    std::cout << "Testing leader AE-reply at compaction boundary with empty log\n";
    NodeInbox ni{};
    auto apply_func = [](FILE* state_machine_fp, const LogEntry& entry) {
        ::fseek(state_machine_fp, 0, SEEK_END);
        int num{69};
        ::fwrite(&num, sizeof(num), 1, state_machine_fp);
        ::fwrite(&num, sizeof(num), 1, state_machine_fp);
    };
    auto create_snapshot = [](FILE* snapshot_fp, FILE* state_machine_fp) {
        constexpr size_t SNAPSHOT_WRITE_BUFFER_SIZE = 1024;
        std::byte buffer[SNAPSHOT_WRITE_BUFFER_SIZE];
        ::rewind(state_machine_fp);
        size_t bytes_read;
        while ((bytes_read = ::fread(buffer, 1, SNAPSHOT_WRITE_BUFFER_SIZE, state_machine_fp)) > 0) {
            ::fwrite(buffer, 1, bytes_read, snapshot_fp);
        }
    };

    Node node{};
    std::optional<std::string> node_err = Node::CreateNode(&node, &ni, apply_func, create_snapshot);
    if (node_err) {
        std::cout << node_err.value() << "\n";
        return 1;
    }

    // White-box: reproduce the user's reported post-compaction leader state.
    // 7 entries were applied and compacted; entries 1..7 live only in the
    // snapshot, so the in-memory log is empty and the next logical index is 8.
    node.log_.clear();
    node.base_logical_idx_ = 8;
    node.base_term_ = 1;
    node.current_term_ = 1;
    node.last_applied_idx_ = 7;
    node.last_applied_term_ = 1;
    node.commit_index_ = 7;
    node.state_ = Node::NodeState::Leader;
    node.next_indexes_.resize(3, 1);
    node.match_indexes_.resize(3, 0);
    node.chunks_sent_.resize(3, 0);
    node.installing_snapshot_ = false;

    // add_peer_if_not_exists initializes a new follower's next_index to
    // log_.size() + base_logical_idx_ == 8, i.e. exactly the snapshot boundary.
    node.next_indexes_[2] = 8;
    node.match_indexes_[2] = 0;

    std::cout << "base_logical_idx_ = " << node.base_logical_idx_ << "\n";
    std::cout << "log_.size() = " << node.log_.size() << "\n";
    std::cout << "next_indexes_[2] = " << node.next_indexes_[2] << "\n";

    // The fresh follower (node 2) rejected the AE heartbeat sent with
    // prev_log_idx == 7 == base_logical_idx_ - 1.
    std::jthread t([&node, &ni]() {
        ni.Push(0, AppendEntriesRespPayload{
            .entries_len = 0,
            .client_fd = 0,
            .server_id = 2,
            .term = 1,
            .success = 0,
        });
        std::this_thread::sleep_for(std::chrono::seconds(1));
        node.Stop();
    });
    node.MainLoop();

    std::cout << "after reply: next_indexes_[2] = " << node.next_indexes_[2] << "\n";
    std::cout << "after reply: installing_snapshot_id_ = " << node.installing_snapshot_ << "\n";

    if (node.next_indexes_[2] == 7 && node.installing_snapshot_ == 2) {
        std::cout << "Test Passed\n";
        return 0;
    }
    std::cout << "Test Failed: follower at snapshot boundary not routed to InstallSnapshot\n";
    return 1;
}
