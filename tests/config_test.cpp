#include "../config.hpp"
#include <iostream>

int main() {
    auto idk = setup_peers();
    std::cout << idk.size() << "\n";
    for (const auto& a : idk) {
        std::cout << "...\n";
    }
    return 0;
}
