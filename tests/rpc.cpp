#include "../core/node.hpp"
#include <thread>

#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    NodeInbox ni{};
    auto apply_func = [](const LogEntry& entry) {

    };
    auto create_snapshot = [](FILE* snapshot_fp, FILE* state_machine_fp) {
        constexpr size_t SNAPSHOT_WRITE_BUFFER_SIZE = 1024;
        std::byte buffer[SNAPSHOT_WRITE_BUFFER_SIZE];
        ::rewind(state_machine_fp);

        size_t bytes_read;
        while ((bytes_read = ::fread(buffer, 1, SNAPSHOT_WRITE_BUFFER_SIZE, state_machine_fp)) > 0) {
              fwrite(buffer, 1, bytes_read, snapshot_fp);
        }
    };

    std::expected<std::unique_ptr<Node>, std::string> n = Node::CreateNode(ni, apply_func, create_snapshot);
    if (!n) {
        #ifdef DEBUG
        std::cout << n.error() << "\n";
        #endif
        return 1;
    }
    std::unique_ptr<Node> node = std::move(*n);
    std::jthread t([&node](){
        std::byte one[CMD_SIZE] = {std::byte{0x00}, std::byte{0x12}, std::byte{0x34}, std::byte{0x56}};
        std::byte two[CMD_SIZE] = {std::byte{0x01}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}};
        std::byte three[CMD_SIZE] = {std::byte{0x02}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};

        auto data = std::vector<std::byte*>{
            one,
            two,
            three
        };
        std::this_thread::sleep_for(std::chrono::seconds(15));
        node->append_commands(data);
    });
    node->MainLoop();

    std::cout << "Test Passed\n";
}
