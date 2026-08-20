#pragma once
#include "../config.hpp"
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

inline int8_t bytes_to_int8(const std::byte* bytes) {
    uint8_t u = std::to_integer<uint8_t>(bytes[0]);
    return static_cast<int8_t>(u);
}

inline int16_t bytes_to_int16(const std::byte* bytes) {
    uint16_t u =
        std::to_integer<uint16_t>(bytes[0]) |
        (std::to_integer<uint16_t>(bytes[1]) << 8);

    return static_cast<int16_t>(u);
}

inline int32_t bytes_to_int32(const std::byte* bytes) {
    uint32_t u =
        std::to_integer<uint32_t>(bytes[0]) |
        (std::to_integer<uint32_t>(bytes[1]) << 8) |
        (std::to_integer<uint32_t>(bytes[2]) << 16) |
        (std::to_integer<uint32_t>(bytes[3]) << 24);

    return static_cast<int32_t>(u);
}

inline int64_t bytes_to_int64(const std::byte* bytes) {
    uint64_t u =
        std::to_integer<uint64_t>(bytes[0]) |
        (std::to_integer<uint64_t>(bytes[1]) << 8) |
        (std::to_integer<uint64_t>(bytes[2]) << 16) |
        (std::to_integer<uint64_t>(bytes[3]) << 24) |
        (std::to_integer<uint64_t>(bytes[4]) << 32) |
        (std::to_integer<uint64_t>(bytes[5]) << 40) |
        (std::to_integer<uint64_t>(bytes[6]) << 48) |
        (std::to_integer<uint64_t>(bytes[7]) << 56);

    return static_cast<int64_t>(u);
}

struct NodeBitset {
    std::vector<uint64_t> cluster; // tracking cluster membership
    std::vector<uint64_t> online; // tracks which nodes are available (i.e. nodes to send messages to)
    size_t num_in_cluster = 0;
    size_t num_available = 0;
    NodeBitset(size_t bits) :
        cluster(((bits-1) / BITS_PER_UINT64_T) + 1),
        online(((bits-1) / BITS_PER_UINT64_T) + 1) {};

    void reset_cluster(uint64_t* ptr, size_t size) {
        cluster.resize(((size-1) / sizeof(uint64_t)) + 1);
        online.resize(((size-1) / sizeof(uint64_t)) + 1);
        std::memcpy(cluster.data(), ptr, size);
        std::memcpy(online.data(), ptr, size);
        num_in_cluster = 0;
        for (uint64_t i : cluster) {
            num_in_cluster += std::popcount(i);
        }
        num_available = num_in_cluster;
    }

    void reset_cluster(std::byte* ptr, size_t size) { // size given in bytes
        cluster.resize(((size-1) / sizeof(uint64_t)) + 1);
        online.resize(((size-1) / sizeof(uint64_t)) + 1);
        std::memcpy(cluster.data(), ptr, size);
        std::memcpy(online.data(), ptr, size);
        num_in_cluster = 0;
        for (uint64_t i : cluster) {
            num_in_cluster += std::popcount(i);
        }
        num_available = num_in_cluster;
    }

    void reset_cluster(FILE* fp, size_t size) {
        cluster.resize(((size-1) / sizeof(uint64_t)) + 1);
        online.resize(((size-1) / sizeof(uint64_t)) + 1);
        ::fread(cluster.data(), size, 1, fp);
        std::memcpy(online.data(), cluster.data(), size);

        num_in_cluster = 0;
        for (uint64_t i : cluster) {
            num_in_cluster += std::popcount(i);
        }
        num_available = num_in_cluster;
    }

    bool is_in_cluster(size_t pos) const {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= cluster.size()) return false;
        return static_cast<bool>((cluster[idx] >> (pos % BITS_PER_UINT64_T)) & 1);
    };

    bool is_available(size_t pos) const {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= online.size()) return false;
        return static_cast<bool>((online[idx] >> (pos % BITS_PER_UINT64_T)) & 1);
    }

    void set_cluster_node(size_t pos) {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= cluster.size()) return;
        if (!((cluster[idx] >> (pos % BITS_PER_UINT64_T)) & 1)) {
            ++num_in_cluster;
        }
        cluster[idx] |= (uint64_t(1) << (pos % BITS_PER_UINT64_T));
    }

    void set_online_node(size_t pos) {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= online.size()) return;
        int prev = !((online[idx] >> (pos % BITS_PER_UINT64_T)) & 1);
        num_available += prev;
        online[idx] |= (uint64_t(1) << (pos % BITS_PER_UINT64_T));
    }

    void set_unavailable_node(size_t pos) {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= online.size()) return;
        int prev = ((online[idx] >> (pos % BITS_PER_UINT64_T)) & 1);
        num_available -= prev;
        online[idx] &= ~(uint64_t(1) << (pos % BITS_PER_UINT64_T)); // unset the bit
    }

    // total number of bits stored in each vector
    size_t bits() const {
        return BITS_PER_UINT64_T * cluster.size();
    }

    size_t bytes() const {
        return sizeof(uint64_t) * cluster.size();
    }

    // number of uint64_t's stored in each vector
    size_t size() const {
        return cluster.size();
    }

};

struct GenericBitset {
    std::vector<uint64_t> v;
    GenericBitset(size_t bits) : v(((bits-1) / BITS_PER_UINT64_T) + 1) {};

    bool is_set(size_t pos) const {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= v.size()) return false;
        return static_cast<bool>((v[idx] >> (pos % BITS_PER_UINT64_T)) & 1);
    }

    void set(size_t pos) {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= v.size()) return;
        v[idx] |= (uint64_t(1) << (pos % BITS_PER_UINT64_T));
    }

    void unset(size_t pos) {
        size_t idx = pos / BITS_PER_UINT64_T;
        if (idx >= v.size()) return;
        v[idx] &= ~(uint64_t(1) << (pos % BITS_PER_UINT64_T)); // unset the bit
    }

    void reset() {
        for (uint64_t& n : v) {
            n = 0;
        }
    }

};
