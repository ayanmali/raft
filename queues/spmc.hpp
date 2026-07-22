#pragma once
#include <atomic>
#include <cstddef>
#include <cstring>

#include <span>
#include <cassert>

#include "../config.hpp"

/*
Single-producer multi-consumer queue that stores fixed size payloads.
The read/write counters grow monotonically; indices into the buffer are derived
with modulo arithmetic so wrap-around is handled transparently.

Capacity should be a power of 2
*/
template <typename T, size_t N>
struct SPMCQueue {
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> read_idx{0};   // owned by consumer
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> write_idx{0};  // owned by producer
    T buffer[N];

    // Returns true on success, false if there is not enough room.
    bool PushOne(const T& data) {
        const uint64_t write = write_idx.load(std::memory_order_relaxed);
        const uint64_t read = read_idx.load(std::memory_order_acquire);

        const uint64_t used = write - read;
        if (used + 1 > N) return false;  // not enough capacity (element count)

        const uint64_t offset = write & (N - 1);
        std::memcpy(&buffer[offset], &data, sizeof(T));

        write_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    // Slot-claim API: single producer reserves the next slot, writes into
    // it directly, then publishes via CommitWrite. Returns nullptr if full.
    T* AcquireWriteSlot() {
        const uint64_t write = write_idx.load(std::memory_order_relaxed);
        const uint64_t read  = read_idx.load(std::memory_order_acquire);
        if (write - read >= N) return nullptr;
        return &buffer[write & (N - 1)];
    }
    void CommitWrite() {
        write_idx.fetch_add(1, std::memory_order_release);
    }

    bool PushMany(std::span<const T> data) {
        //if (data.size() > N) return false;  // message does not fit at all
        const uint64_t write = write_idx.load(std::memory_order_relaxed);
        const uint64_t read = read_idx.load(std::memory_order_acquire);

        const uint64_t used = write - read;
        if (used + data.size() > N) return false;  // not enough capacity

        // Copy elements one by one, handling wrap-around
        const uint64_t offset = write & (N - 1);
        CopyIn(buffer, N, offset, data.data(), data.size() * sizeof(T));

        write_idx.fetch_add(data.size(), std::memory_order_release);
        return true;
    }

    // Returns std::nullopt if there is no message available.
    bool Pop(T* payload) {
        while (true) {
            uint64_t read = read_idx.load(std::memory_order_relaxed);
            const uint64_t write = write_idx.load(std::memory_order_acquire);
            if (read == write) return false;

            // Speculatively read data BEFORE advancing read_idx.
            // This is safe because while read_idx == read, the producer
            // cannot have wrapped around to overwrite this slot (the
            // buffer would appear full from the producer's perspective).
            const uint64_t offset = read & (N - 1);
            std::memcpy(payload, &buffer[offset], sizeof(T));

            if (read_idx.compare_exchange_weak(
                read, read + 1, std::memory_order_release, std::memory_order_relaxed)) {
                    return true;
                }
            // CAS failed — another consumer claimed this slot; retry.
        }
    }

};

// Helper function to compare two sequences of values
// template <typename T>
// bool CompareSequence(const std::vector<T>& a, const std::vector<T>& b) {
//     if (a.size() != b.size()) return false;
//     for (size_t i = 0; i < a.size(); ++i) {
//         if (a[i] != b[i]) return false;
//     }
//     return true;
// }
