#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>
#include <type_traits>
#include <utility>
#include <unistd.h>
#include <sys/eventfd.h>

#include "./mpsc.hpp"
#include "./spmc.hpp"

static constexpr int SPIN_LOWER_BOUND = 32;
static constexpr int SPIN_UPPER_BOUND = 128;

/*
Task and Completion both carry their payload either inline (zero allocation)
or via a heap pointer (allocated by the producer, freed by the consumer)
when the payload exceeds INLINE_PAYLOAD_BYTES. Producers write into the queue
slot directly via AcquireWriteSlot/CommitWrite, eliminating the
local-then-memcpy step that the old PushOne(const T&) API required.

Both structs must stay trivially copyable so the queues' memcpy semantics work.
*/
struct Task {
    uint64_t   conn_id;
    uint32_t   seq;
    uint32_t   len;
    std::byte* heap_data;                                // null when inline
    alignas(8) std::byte inline_data[INLINE_PAYLOAD_BYTES];
};
static_assert(std::is_trivially_copyable_v<Task>);

struct Completion {
    uint64_t   conn_id;
    uint32_t   seq;
    uint32_t   len;
    std::byte* heap_data;
    alignas(8) std::byte inline_data[INLINE_PAYLOAD_BYTES];
};
static_assert(std::is_trivially_copyable_v<Completion>);

inline std::byte* payload(Task& t) {
    return t.heap_data ? t.heap_data : t.inline_data;
}
inline const std::byte* payload(const Completion& c) {
    return c.heap_data ? c.heap_data : c.inline_data;
}

/*
ThreadPool is templated on the handler type H so the handler is stored by its
real type. The worker's call site is a direct call the compiler can inline,
with no std::function-style indirect dispatch and no heap allocation.

H must be callable as:
    uint32_t H(const std::byte* req, uint32_t req_len,
               std::byte* out, uint32_t out_cap)

The handler writes the response into out[0..out_cap) and returns the actual
response length. If the response exceeds out_cap, the handler must NOT write
past out_cap and must still return the required length; the caller will
reallocate with a sufficiently large buffer and call again.

The handler is invoked concurrently from worker threads; it must be
thread-safe or dispatch to a single-threaded apply layer. It must also be
deterministic across the size-probe and the actual write when a heap fallback
is needed.
*/
template <typename H>
struct ThreadPool {
    static_assert(
        std::is_invocable_r_v<uint32_t, H&, const std::byte*, uint32_t,
                              std::byte*, uint32_t>,
        "Handler must be callable as "
        "uint32_t(const std::byte*, uint32_t, std::byte*, uint32_t)");

    SPMCQueue<Task, TASK_QUEUE_N>                 tasks;
    MPSC<Completion, COMP_QUEUE_N, POOL_P>        completions;
    int                                           loop_event_fd; // owned externally
    std::array<std::thread, POOL_P>               workers;
    std::atomic<bool>                             running{false};
    H                                             handler;

    template <typename U>
    ThreadPool(int event_fd, U&& h)
        : loop_event_fd(event_fd), handler(std::forward<U>(h)) {}

    ~ThreadPool() { Stop(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    void Start() {
        running.store(true, std::memory_order_release);
        for (size_t i = 0; i < POOL_P; ++i) {
            workers[i] = std::thread(&ThreadPool::WorkerMain, this, i);
        }
    }

    void Stop() {
        if (!running.exchange(false, std::memory_order_acq_rel)) return;
        for (auto& w : workers) {
            if (w.joinable()) w.join();
        }
    }

    // Consumer side: called by the event loop after eventfd fires.
    template <typename F>
    size_t DrainCompletions(F&& fn) {
        return completions.DrainAll(std::forward<F>(fn));
    }

private:
    void WorkerMain(size_t my_id) {
        Task t;
        unsigned spins = 0;
        while (running.load(std::memory_order_acquire)) {
            if (!tasks.Pop(&t)) {
                if (spins < SPIN_LOWER_BOUND) {
                    ++spins;
                } else if (spins < SPIN_UPPER_BOUND) {
                    std::this_thread::yield();
                    ++spins;
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
                continue;
            }
            spins = 0;

            const std::byte* req     = payload(t);
            const uint32_t   req_len = t.len;

            // Acquire a completion slot up front so the handler can write
            // its response directly into the slot's inline buffer.
            Completion* slot = nullptr;
            for (;;) {
                slot = completions.qs[my_id].AcquireWriteSlot();
                if (slot) break;
                std::this_thread::yield();
                if (!running.load(std::memory_order_acquire)) {
                    if (t.heap_data) delete[] t.heap_data;
                    return;
                }
            }
            slot->conn_id   = t.conn_id;
            slot->seq       = t.seq;
            slot->heap_data = nullptr;

            uint32_t n = handler(req, req_len, slot->inline_data, INLINE_PAYLOAD_BYTES);
            if (n > INLINE_PAYLOAD_BYTES) {
                // Response didn't fit inline. Allocate exactly once and
                // re-run the handler. Handlers are required to be
                // deterministic across these two calls.
                slot->heap_data = new std::byte[n];
                slot->len       = handler(req, req_len, slot->heap_data, n);
            } else {
                slot->len = n;
            }
            completions.qs[my_id].CommitWrite();

            if (t.heap_data) {
                delete[] t.heap_data;
                t.heap_data = nullptr;
            }

            // Doorbell. eventfd is a counter, so concurrent writes coalesce
            // and one read on the loop side returns the running sum.
            const uint64_t one = 1;
            (void)::write(loop_event_fd, &one, sizeof one);
        }
    }
};
