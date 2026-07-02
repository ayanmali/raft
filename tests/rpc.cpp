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
        std::byte one[CMD_SIZE] = {std::byte{0x01}, std::byte{0x02}, std::byte{0x03}};
        std::byte two[CMD_SIZE] = {std::byte{0x42}, std::byte{0x69}, std::byte{0x67}, std::byte{0x91}};
        std::byte three[CMD_SIZE] = {std::byte{0xAB}, std::byte{0xCD}, std::byte{0xEF}};

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
