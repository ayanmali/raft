#include "../core/node.hpp"
#include "../rpc/event_loop/event_loop.hpp"
#include <thread>

#include <iostream>
int main() {
    std::cout << "Testing follower apply-after-compact\n";
    NodeInbox ni{};
    auto apply_func = [](FILE* state_machine_fp, const LogEntry& entry) {
        ::fseek(state_machine_fp, 0, SEEK_END);
        int num{69};
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

    std::jthread t([&node, &ni](){
        std::byte one[CMD_SIZE] = {std::byte{0x00}, std::byte{0x12}, std::byte{0x34}, std::byte{0x56}};
        std::byte two[CMD_SIZE] = {std::byte{0x01}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}};
        std::byte three[CMD_SIZE] = {std::byte{0x02}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};
        LogEntry e1(one, CMD_SIZE, 1);
        LogEntry e2(two, CMD_SIZE, 1);
        LogEntry e3(three, CMD_SIZE, 1);

        // AE #1: leader 0 ships the first 3 entries. prev_log_idx == base_logical_idx_ - 1 == 0,
        // so prev_log_term is validated against base_term_ (0).
        auto p1 = AppendEntriesReqPayload{3, 0, 1, 0, 0, 0, 0};
        std::memcpy(p1.entries, &e1, sizeof(LogEntry) * 3);
        ni.Push(0, p1);

        // AE #2: 1 more entry; prev_log_idx == 3, term 1 matches log_[2]. Leader commits through 3.
        auto p2 = AppendEntriesReqPayload{1, 0, 1, 0, 3, 1, 3};
        std::memcpy(p2.entries, &e1, sizeof(LogEntry));
        ni.Push(0, p2);

        // AE #3: prev_log_idx == 4, term 1 matches log_[3]. 3 new entries, leader_commit == 4.
        // Log grows to 7 entries >= LOG_COMPACT_THRESHOLD. Compaction runs in the same loop
        // iteration the entries are appended, before the apply block catches up on the next.
        auto p3 = AppendEntriesReqPayload{3, 0, 1, 0, 4, 1, 4};
        std::memcpy(p3.entries, &e1, sizeof(LogEntry) * 3);
        ni.Push(0, p3);

        std::this_thread::sleep_for(std::chrono::seconds(3));
        node.Stop();
    });
    node.MainLoop();

    std::cout << "Test Passed\n";
}
