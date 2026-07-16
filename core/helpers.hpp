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

struct DynamicBitset {
    std::vector<uint8_t> v; // Each node has its own ID set to false
    size_t num_set = 0;
    DynamicBitset(size_t sz) : v(((sz-1) / BITS_PER_BYTE) + 1) {};

    void reset(uint8_t* ptr, size_t size) {
        v.resize(size);
        std::memcpy(v.data(), ptr, size);
        num_set = 0;
        for (uint8_t i : v) {
            num_set += std::popcount(i);
        }
    }

    bool operator[](size_t pos) {
        size_t idx = pos / BITS_PER_BYTE;
        return static_cast<bool>((v[idx] >> pos) & 1);
    };

    void set(size_t pos) {
        size_t idx = pos / BITS_PER_BYTE;
        int prev = (v[idx] >> pos) & 1;
        v[idx] |= (1 << pos);
        if (!prev) ++num_set;
    }

    void unset(size_t pos) {
        size_t idx = pos / BITS_PER_BYTE;
        int prev = (v[idx] >> pos) & 1;
        v[idx] |= (1 << pos); // set the bit
        v[idx] ^= (1 << pos); // flip it
        if (prev) --num_set;
    }

    // void flip(size_t pos) {
    //     size_t idx = pos / 8;
    //     v[idx] ^= (1 << pos);
    // }

    void add(size_t id) {
        size_t idx = id / BITS_PER_BYTE;
        //if (idx < v.size()) return;
        if (idx >= v.size()) v.resize(idx + 1);

        int prev = (v[idx] >> id) & 1;
        v[idx] |= (1 << id);
        if (!prev) ++num_set;
    }

    void resize_bytes(size_t bytes) {
        v.resize(bytes);
    }

    size_t size() {
        return num_set;
    }

    size_t total_size() {
        return v.size();
    }

    size_t total_bits() {
        return v.size() * BITS_PER_BYTE;
    }

    bool empty() {
        return num_set == 0;
    }

    uint8_t* data() {
        return v.data();
    }
};
