#include <atomic>
#include <cstddef>
#include <cstring>
#include <cassert>
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

    bool PushOne(T&& data) {
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read  = read_idx.load(std::memory_order_acquire);

        if (write - read >= N) return false;

        buffer[write & (N - 1)] = std::forward<T>(data);

        write_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    template<typename... Args>
    bool EmplaceOne(Args&&... args) {
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read  = read_idx.load(std::memory_order_acquire);

        if (write - read >= N) return false;

        buffer[write & (N - 1)] =
            T(std::forward<Args>(args)...);

        write_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    bool PopOne(T* out) {
        const size_t read  = read_idx.load(std::memory_order_relaxed);
        const size_t write = write_idx.load(std::memory_order_acquire);
        if (read == write) return false;
        *out = std::move(buffer[read & (N - 1)]);
        // buffer[offset] is now in a valid-but-moved-from state (e.g. null
        // unique_ptr). Next producer overwrites it via move-assign above.
        read_idx.fetch_add(1, std::memory_order_release);
        return true;
    }

    bool PopOne() {
        const size_t read  = read_idx.load(std::memory_order_relaxed);
        const size_t write = write_idx.load(std::memory_order_acquire);
        if (read == write) return false;
        T out = std::move(buffer[read & (N - 1)]);
        // buffer[offset] is now in a valid-but-moved-from state (e.g. null
        // unique_ptr). Next producer overwrites it via move-assign above.
        read_idx.fetch_add(1, std::memory_order_release);
        return true;
    }


    // Sweep every sub-queue once, invoking callback for each item drained.
    // Returns the total number of items processed.
    template <typename F>
    size_t DrainAll(F&& callback) {
        size_t total = 0;
        T item;
        while (PopOne(&item)) {
            callback(std::move(item));
            ++total;
        }

        return total;
    }

};
