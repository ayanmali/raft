#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
Task: a unit of work for a worker. Owns the request bytes (heap-allocated by
the event loop, freed by the worker after the handler runs).

Completion: a unit of result. Owns the response bytes (heap-allocated by the
worker, freed by the event loop after appending to the connection's wbuf).

Both must remain trivially copyable so they fit through SPSCQueue/SPMCQueue.
*/
struct Task {
    uint64_t   conn_id;
    uint32_t   seq;
    uint32_t   len;
    std::byte* data;
};
static_assert(std::is_trivially_copyable_v<Task>);

struct Completion {
    uint64_t   conn_id;
    uint32_t   seq;
    uint32_t   len;
    std::byte* data;
};
static_assert(std::is_trivially_copyable_v<Completion>);

// Owned heap buffer returned by a handler. Caller takes ownership and is
// responsible for `delete[] data`.
struct OwnedBytes {
    std::byte* data;
    uint32_t   len;
};

/*
ThreadPool is templated on the handler type H so the handler is stored by its
real type. The worker's call site is a direct call the compiler can inline,
with no std::function-style indirect dispatch and no heap allocation.

H must be callable as: OwnedBytes(const std::byte* req, uint32_t req_len).
The handler is invoked concurrently from worker threads; it must be
thread-safe or dispatch to a single-threaded apply layer.
*/
template <typename H>
struct ThreadPool {
    static_assert(
        std::is_invocable_r_v<OwnedBytes, H&, const std::byte*, uint32_t>,
        "Handler must be callable as OwnedBytes(const std::byte*, uint32_t)");

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

    // Producer side: called by the event loop to dispatch a task.
    // Returns false if the task queue is full (apply backpressure upstream).
    bool Submit(const Task& t) { return tasks.PushOne(t); }

    // Consumer side: called by the event loop after eventfd fires.
    template <typename F>
    size_t DrainCompletions(F&& fn) {
        return completions.DrainAll(std::forward<F>(fn));
    }

private:
    void WorkerMain(size_t my_id) {
        Task t;
        // Adaptive backoff: tight spin -> yield -> short sleep.
        // Keeps idle pools cheap without adding a condvar yet.
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

            OwnedBytes resp = handler(t.data, t.len);
            delete[] t.data;

            Completion c{t.conn_id, t.seq, resp.len, resp.data};
            // Push into our own SPSC sub-queue. In rare full conditions
            // (consumer is slow), spin briefly. Never block the loop's fd.
            while (!completions.Push(my_id, c)) {
                std::this_thread::yield();
                if (!running.load(std::memory_order_acquire)) {
                    delete[] resp.data;
                    return;
                }
            }

            // Doorbell. eventfd is a counter, so concurrent writes coalesce
            // and one read on the loop side returns the running sum.
            const uint64_t one = 1;
            (void)::write(loop_event_fd, &one, sizeof one);
        }
    }
};
