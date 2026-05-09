#pragma once
#include <cstddef>
#include <type_traits>

#include "./spsc.hpp"

/*
Multi-producer single-consumer queue built as P independent SPSC sub-queues.
- Each producer is assigned a stable id in [0, P) at thread-spawn time and
  pushes only into qs[id]. Producers never contend with each other.
- The single consumer round-robins across the sub-queues; the rotating
  start cursor avoids permanent starvation of higher-indexed producers.

Constraints:
- T must be trivially copyable (SPSCFixedSize uses memcpy).
- N (capacity per sub-queue) and P (producer count) must both be powers of 2.
*/
template <typename T, size_t N, size_t P>
struct MPSC {
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");
    static_assert(N > 0 && (N & (N - 1)) == 0, "N must be a power of 2");
    static_assert(P > 0 && (P & (P - 1)) == 0, "P must be a power of 2");

    SPSCQueue<T, N> qs[P];

    bool Push(size_t producer_id, const T& v) {
        return qs[producer_id].PushOne(v);
    }

    bool Pop(T* out) {
        for (size_t i = 0; i < P; ++i) {
            const size_t idx = (start_ + i) & (P - 1);
            if (qs[idx].PopOne(out)) {
                start_ = (idx + 1) & (P - 1);
                return true;
            }
        }
        return false;
    }

    // Sweep every sub-queue once, invoking callback for each item drained.
    // Returns the total number of items processed.
    template <typename F>
    size_t DrainAll(F&& callback) {
        size_t total = 0;
        T item;
        for (size_t i = 0; i < P; ++i) {
            while (qs[i].PopOne(&item)) {
                callback(item);
                ++total;
            }
        }
        return total;
    }

private:
    size_t start_ = 0;
};
