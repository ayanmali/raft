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
    uint8_t cluster[8] = {0x01}; // single-node cluster config (leader's node_ids_.bytes() == 8)
    uint8_t state[40];
    for (int i = 0; i < 10; ++i) {
        state[i * 4 + 0] = 0x45;
        state[i * 4 + 1] = 0x00;
        state[i * 4 + 2] = 0x00;
        state[i * 4 + 3] = 0x00;
    }

    // Reproduce the leader's InstallSnapshot framing: chunk 0 carries the
    // cluster-config header (size_t + cluster bitset) and (being the first
    // chunk) shares its CHUNK budget with that header, so it holds no data
    // here. Every later chunk carries SNAPSHOT_CHUNK_SIZE bytes of data; the
    // final chunk may be partial (8 bytes).
    const size_t cluster_bytes = sizeof(cluster); // leader's node_ids_.bytes(); receiver derives sm_header_bytes() from it
    InstallSnapshotReqPayload p0{};
    p0.data_len = sizeof(state);
    p0.term = 1;
    p0.leader_id = 0;
    p0.last_included_idx = 10;
    p0.last_included_term = 1;
    p0.offset = 0;
    p0.done = 0;
    p0.dest_id = 0;
    std::memcpy(p0.partial_state, &cluster_bytes, sizeof(size_t));
    std::memcpy(p0.partial_state + sizeof(size_t), cluster, sizeof(cluster));
    ni.Push(0, p0);

    // chunk 0 held no data; chunks 1..NUM carry data_off (k-1)*CHUNK each.
    const int num_chunks = static_cast<int>((sizeof(state) + SNAPSHOT_CHUNK_SIZE - 1) / SNAPSHOT_CHUNK_SIZE);
    for (int k = 1; k <= num_chunks; ++k) {
        const size_t data_off = static_cast<size_t>(k - 1) * SNAPSHOT_CHUNK_SIZE;
        const size_t amount = std::min<size_t>(SNAPSHOT_CHUNK_SIZE, sizeof(state) - data_off);
        InstallSnapshotReqPayload p{};
        p.data_len = sizeof(state);
        p.term = 1;
        p.leader_id = 0;
        p.last_included_idx = 10;
        p.last_included_term = 1;
        p.offset = static_cast<uint64_t>(k) * SNAPSHOT_CHUNK_SIZE;
        p.done = (data_off + amount >= sizeof(state)) ? 1 : 0;
        p.dest_id = 0;
        std::memcpy(p.partial_state, state + data_off, amount);
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

    if (sm_st.st_size != static_cast<off_t>(node.sm_header_bytes() + sizeof(state)) || node.last_applied_idx_ != 10) {
        std::cout << "Test Failed: state machine not restored from installed snapshot\n";
        return 1;
    }

    // verify bytes match the snapshot state
    ::rewind(node.sm_fp_);
    ::fseek(node.sm_fp_, static_cast<long>(node.sm_header_bytes()), SEEK_SET);
    uint8_t readback[40];
    ::fread(readback, 1, sizeof(readback), node.sm_fp_);
    if (std::memcmp(readback, state, sizeof(state)) != 0) {
        std::cout << "Test Failed: restored state machine bytes differ from snapshot\n";
        return 1;
    }

    std::cout << "Test Passed\n";
    return 0;
}