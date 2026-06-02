#include "../core/node.hpp"
#include <iostream>
int main() {
    std::cout << "Testing RPC correctness\n";
    NodeInbox ni{};
    Node node{ni};

    std::cout << "Test Passed\n";
}