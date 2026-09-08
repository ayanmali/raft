#include "../core/helpers.hpp"
#include <iostream>
#include <cstdint>
#include <cstring>

int main() {
    NodeBitset d{16};
    d.set_cluster_node(0);

    d.set_cluster_node(3);

    d.set_cluster_node(1);
    d.set_online_node(1);

    d.set_cluster_node(5);
    d.set_cluster_node(4);
    d.set_cluster_node(11);
    d.set_cluster_node(14);
    d.set_online_node(14);
    d.set_cluster_node(9);

    {uint64_t bitset;
    for (size_t k = 0; k < d.online.size(); ++k) {
        bitset = d.online[k];
        while (bitset != 0) {
          uint64_t t = bitset & -bitset;
          int r = __builtin_ctzl(bitset);
          std::cout << (k * 64 + r) << ", ";
          bitset ^= t;
        }
    }
    std::cout << "\n";}

    {uint64_t bitset;
    for (size_t k = 0; k < d.cluster.size(); ++k) {
        bitset = d.cluster[k];
        while (bitset != 0) {
          uint64_t t = bitset & -bitset;
          int r = __builtin_ctzl(bitset);
          std::cout << (k * 64 + r) << ", ";
          bitset ^= t;
        }
    }
    std::cout << "\n";}

}
