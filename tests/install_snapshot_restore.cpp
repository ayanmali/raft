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

// Regression test for the missing InstallSnapshot state-machine restore.
//
// A follower that installs a snapshot advances last_applied_idx_ to the
// snapshot's last_included_idx, but its state machine file must ALSO be
// rebuilt from the snapshot's state data. Otherwise stale/shorter state
// machine data is kept and later snapshots become inconsistent.
int main() {
    std::cout << "Testing InstallSnapshot state machine restore\n";
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

    // Seed a stale state machine: only 2 entries applied (8 bytes).
    LogEntry e(1);
    node.apply_entry(node.sm_fp_, e);
    node.apply_entry(node.sm_fp_, e);
    ::fflush(node.sm_fp_);
    node.last_applied_idx_ = 2;

    // Fabricate a leader snapshot: last_included_idx=10, 10 entries = 40 bytes
    // of state (4 bytes each, int 69 = 0x45 bytes).
    uint8_t cluster[1] = {0x07};
    uint8_t state[40];
    for (int i = 0; i < 10; ++i) {
        state[i * 4 + 0] = 0x45;
        state[i * 4 + 1] = 0x00;
        state[i * 4 + 2] = 0x00;
        state[i * 4 + 3] = 0x00;
    }

    const int num_chunks = sizeof(state) / SNAPSHOT_CHUNK_SIZE;
    for (int c = 0; c < num_chunks; ++c) {
        InstallSnapshotReqPayload p{};
        p.term = 1;
        p.leader_id = 0;
        p.last_included_idx = 10;
        p.last_included_term = 1;
        p.cluster_raw_size = sizeof(cluster);
        p.offset = static_cast<uint64_t>(c) * SNAPSHOT_CHUNK_SIZE;
        p.done = (c == num_chunks - 1) ? 1 : 0;
        p.dest_id = 0;
        std::memcpy(p.cluster, cluster, sizeof(cluster));
        std::memcpy(p.partial_state, state + c * SNAPSHOT_CHUNK_SIZE, SNAPSHOT_CHUNK_SIZE);
        ni.Push(0, p);
    }

    std::jthread t([&node]() {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        node.Stop();
    });
    node.MainLoop();

    struct stat sm_st;
    ::stat(STATE_MACHINE_FILE_PATH, &sm_st);
    std::cout << "state machine file size = " << sm_st.st_size << "\n";
    std::cout << "last_applied_idx_ = " << node.last_applied_idx_ << "\n";

    if (sm_st.st_size != static_cast<off_t>(sizeof(state)) || node.last_applied_idx_ != 10) {
        std::cout << "Test Failed: state machine not restored from installed snapshot\n";
        return 1;
    }

    // verify bytes match the snapshot state
    ::rewind(node.sm_fp_);
    uint8_t readback[40];
    ::fread(readback, 1, sizeof(readback), node.sm_fp_);
    if (std::memcmp(readback, state, sizeof(state)) != 0) {
        std::cout << "Test Failed: restored state machine bytes differ from snapshot\n";
        return 1;
    }

    std::cout << "Test Passed\n";
    return 0;
}