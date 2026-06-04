#include "../core/node.hpp"

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
    std::unique_ptr<Node> node = std::move(n.value());
    node->MainLoop();

    std::cout << "Test Passed\n";
}
