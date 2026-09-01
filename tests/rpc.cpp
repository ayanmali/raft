#include "../core/node.hpp"
#include "../rpc/event_loop/event_loop.hpp"
#include <thread>

#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    NodeInbox ni{};
    auto apply_func = [](FILE* state_machine_fp, const LogEntry& entry) {
        ::fseek(state_machine_fp, 0, SEEK_END);
        int num{69};
        ::fwrite(&num, sizeof(num), 1, state_machine_fp);
    };

    Node node{};
    std::optional<std::string> node_err = Node::CreateNode(&node, &ni, apply_func);
    if (node_err) {
        #ifdef DEBUG
        std::cout << node_err.value() << "\n";
        #endif
        return 1;
    }
    std::jthread t([&node](){
        std::byte one[CMD_SIZE] = {std::byte{0x00}, std::byte{0x12}, std::byte{0x34}, std::byte{0x56}};
        std::byte two[CMD_SIZE] = {std::byte{0x01}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}};
        std::byte three[CMD_SIZE] = {std::byte{0x02}, std::byte{0x11}, std::byte{0x22}, std::byte{0x33}};

        auto data = std::vector<std::byte*>{
            one,
            two,
            three
        };
        std::this_thread::sleep_for(std::chrono::seconds(5));
        node.append_commands(data);
        std::this_thread::sleep_for(std::chrono::seconds(30));
        node.Stop();
    });
    node.MainLoop();

    std::cout << "Test Passed\n";
}
