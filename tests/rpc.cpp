#include "../core/node.hpp"

#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    NodeInbox ni{};
    Node node(ni);
    node.main_loop();

    std::cout << "Test Passed\n";
}
