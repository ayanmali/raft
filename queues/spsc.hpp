#include <atomic>
#include <cstddef>
#include <cstring>
#include <span>
#include <vector>
#include <cassert>
#include "./utils.hpp"
#include "../config.hpp"

/*
Single-producer single-consumer queue that stores fixed length byte payloads.
Each message is laid out as: [size_t payload_size][payload bytes...].
The read/write counters grow monotonically; indices into the buffer are derived
with modulo arithmetic so wrap-around is handled transparently.

Uses fixed-size messages

Capacity should be a power of 2
*/
template <typename T, size_t N>
struct SPSCQueue {
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> read_idx{0};   // owned by consumer
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> write_idx{0};  // owned by producer
    T buffer[N];
    // std::byte buffer[N]{};

    // Returns true on success, false if there is not enough room.
    bool PushOne(const T& data) {
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read = read_idx.load(std::memory_order_acquire);

        const size_t used = write - read;
        if (used + 1 > N) return false;  // not enough capacity (element count)

        const size_t offset = write & (N - 1);
        std::memcpy(&buffer[offset], &data, sizeof(T));

        write_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    // template<typename F>
    // bool PushOne(const T& data, F&& copy_func) {
    //     const size_t write = write_idx.load(std::memory_order_relaxed);
    //     const size_t read = read_idx.load(std::memory_order_acquire);

    //     const size_t used = write - read;
    //     if (used + 1 > N) return false;  // not enough capacity (element count)

    //     const size_t offset = write & (N - 1);
    //     copy_func(&buffer[offset]); // copies data into the buffer

    //     write_idx.fetch_add(1, std::memory_order_release);
    //     return true;
    // }

    // Slot-claim API: producer reserves the next slot, writes into it
    // directly (avoiding the local-then-memcpy overhead of PushOne), then
    // publishes via CommitWrite. Returns nullptr if the queue is full.
    T* AcquireWriteSlot() {
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read  = read_idx.load(std::memory_order_acquire);
        if (write - read >= N) return nullptr;
        return &buffer[write & (N - 1)];
    }
    void CommitWrite() {
        write_idx.fetch_add(1, std::memory_order_release);
    }

    bool PushMany(std::span<const T> data) {
        //if (data.size() > N) return false;  // message does not fit at all
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read = read_idx.load(std::memory_order_acquire);

        const size_t used = write - read;
        if (used + data.size() > N) return false;  // not enough capacity
        
        // Copy elements one by one, handling wrap-around
        const size_t offset = write & (N - 1);
        Queues::CopyIn(buffer, N, offset, data.data(), data.size() * sizeof(T));

        write_idx.fetch_add(data.size(), std::memory_order_release);
        return true;
    }

    // Returns an empty object if there is no message available.
    bool PopOne(T* payload) {
        const size_t read = read_idx.load(std::memory_order_relaxed);
        const size_t write = write_idx.load(std::memory_order_acquire);
        if (read == write) return false;

        const size_t offset = read & (N - 1);
        std::memcpy(payload, &buffer[offset], sizeof(T));

        read_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    std::vector<T> PopMany(const size_t num_elements) {
        const size_t read = read_idx.load(std::memory_order_relaxed);
        const size_t write = write_idx.load(std::memory_order_acquire);
        if (read == write) return std::vector<T>{};

        if (read + num_elements > write) return std::vector<T>{};  // incomplete write

        // Copy elements one by one, handling wrap-around
        std::vector<T> payload(num_elements);
        const size_t offset = read & (N - 1);
        Queues::CopyOut(buffer, N, offset, payload.data(), num_elements * sizeof(T));

        read_idx.fetch_add(num_elements, std::memory_order_release);
        return payload;
    }
};