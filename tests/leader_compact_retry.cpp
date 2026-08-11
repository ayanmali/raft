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

// Regression test for the leader AE-reply OOB crash after log compaction.
//
// A follower's next_index can be decremented below the compaction boundary
// (base_logical_idx_) when it keeps rejecting AppendEntries. The AE-reply
// failure path must route such followers through InstallSnapshot instead of
// retrying send_append_entries, whose prev_log_idx - base_logical_idx_
// arithmetic underflows (both operands are uint32_t) and indexes log_ OOB.
int main() {
    std::cout << "Testing leader AE-reply retry below compaction boundary\n";
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

    // White-box: construct the exact post-compaction leader state.
    // Compaction left a snapshot covering logical indices 1..4
    // (base_logical_idx_ = 5), with 3 log entries at logical indices 5,6,7.
    node.log_ = {LogEntry(1), LogEntry(1), LogEntry(1)};
    node.base_logical_idx_ = 5;
    node.base_term_ = 1;
    node.current_term_ = 1;
    node.last_applied_idx_ = 4;
    node.last_applied_term_ = 1;
    node.commit_index_ = 4;
    node.state_ = Node::NodeState::Leader;
    node.next_indexes_.resize(3, 1);
    node.match_indexes_.resize(3, 0);
    node.chunks_sent_.resize(3, 0);
    node.installing_snapshot_id_ = -1;

    // follower 2's next_index sits at logical index 2: it is behind the
    // snapshot (needs data at/under index 4-like behaviour).
    node.next_indexes_[2] = 2;
    node.match_indexes_[2] = 1;

    std::cout << "base_logical_idx_ = " << node.base_logical_idx_ << "\n";
    std::cout << "log_.size() = " << node.log_.size() << "\n";
    std::cout << "next_indexes_[2] = " << node.next_indexes_[2] << "\n";

    bool done = false;
    std::jthread t([&node, &ni, &done]() {
        ni.Push(0, AppendEntriesRespPayload{
            .entries_len = 0,
            .client_fd = 0,
            .server_id = 2,
            .term = 1,
            .success = 0,
        });
        std::this_thread::sleep_for(std::chrono::seconds(1));
        done = true;
        node.Stop();
    });
    node.MainLoop();
    (void)done;

    std::cout << "after reply: next_indexes_[2] = " << node.next_indexes_[2] << "\n";
    std::cout << "after reply: installing_snapshot_id_ = " << node.installing_snapshot_id_ << "\n";

    if (node.next_indexes_[2] == 1 && node.installing_snapshot_id_ == 2) {
        std::cout << "Test Passed\n";
        return 0;
    }
    std::cout << "Test Failed: follower behind compaction boundary not routed to InstallSnapshot\n";
    return 1;
}