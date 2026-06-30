#include "../core/node.hpp"
#include <thread>

#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    NodeInbox ni{};
    std::expected<std::unique_ptr<Node>, std::string> n = Node::CreateNode(ni);
    if (!n) {
        #ifdef DEBUG
        std::cout << n.error() << "\n";
        #endif
        return 1;
    }
    std::unique_ptr<Node> node = std::move(*n);
    std::jthread t([&node](){
        // dummy data
        auto data = std::vector<std::vector<std::byte>>{
            {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}},
            {std::byte{0xFF}, std::byte{0x00}},
            {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}, std::byte{0x40}}
        };
        std::this_thread::sleep_for(std::chrono::seconds(15));
        node->append_commands(data);
    });
    node->MainLoop();

    std::cout << "Test Passed\n";
}
