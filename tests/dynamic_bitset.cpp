#include "../core/helpers.hpp"
#include <iostream>
#include <cstdint>
#include <vector>
#include <cstring>

int main() {
    DynamicBitset d{16};
    d.set(0);
    d.add(1);
    d.set(5);
    d.set(3);
    d.set(11);
    d.set(14);
    d.set(9);
    uint8_t n[2];
    std::memcpy(&n, d.v.data(), sizeof(n));
    for (auto x : n) {
        std::cout << static_cast<int>(x) << "\n";
    }
    // std::cout << d.num_set << "\n";
    // for (int id = 0; id < d.total_bits(); ++id) {
    //     if (!d[id]) continue;
    //     std::cout << id << ", ";
    // }
    // std::cout << "\n";
    // 0010 1011 0000 0000
}
