#include <atomic>
#include <cstddef>
#include <cstring>
#include <span>
#include <vector>
#include <cassert>
#include "./utils.hpp"
#include "../config.hpp"

/*
Single-producer single-consumer queue that stores variable length byte payloads.
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

    bool PushMany(std::span<const T> data) {
        //if (data.size() > N) return false;  // message does not fit at all
        const size_t write = write_idx.load(std::memory_order_relaxed);
        const size_t read = read_idx.load(std::memory_order_acquire);

        const size_t used = write - read;
        if (used + data.size() > N) return false;  // not enough capacity
        
        // Copy elements one by one, handling wrap-around
        const size_t offset = write & (N - 1);
        CopyIn(buffer, N, offset, data.data(), data.size() * sizeof(T));

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
        CopyOut(buffer, N, offset, payload.data(), num_elements * sizeof(T));

        read_idx.fetch_add(num_elements, std::memory_order_release);
        return payload;
    }
};

// TODO: refactor Pop() calls

// Helper function to compare two sequences of values
// template <typename T>
// bool CompareSequence(const std::vector<T>& a, const std::vector<T>& b) {
//     if (a.size() != b.size()) return false;
//     for (size_t i = 0; i < a.size(); ++i) {
//         if (a[i] != b[i]) return false;
//     }
//     return true;
// }

// int main() {
//     bool all_tests_passed = true;
//     int test_count = 0;
//     int pass_count = 0;

//     // Test 1: Basic push and pop
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Basic push and pop\n";
//         SPSCFixedSize<int, 64> queue;

//         int value = 42;

//         if (!queue.PushOne(value)) {
//             std::cerr << "  ERROR: Failed to push value\n";
//             all_tests_passed = false;
//         } else {
//             int popped = queue.PopOne();
//             std::cout << "Test 1 - popped = " << popped << "\n";
//             if (!popped) {
//                 std::cerr << "  ERROR: Queue unexpectedly empty\n";
//                 all_tests_passed = false;
//             } else if (popped != value) {
//                 std::cerr << "  ERROR: Popped value doesn't match pushed value\n";
//                 std::cerr << "  Expected: " << value << "\n";
//                 std::cerr << "  Got:      " << popped << "\n";
//                 all_tests_passed = false;
//             } else {
//                 std::cout << "  PASS\n";
//                 ++pass_count;
//             }
//         }
//     }

//     // Test 2: Multiple messages in sequence
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Multiple values in sequence\n";
//         SPSCFixedSize<int, 128> queue;

//         std::vector<int> values = {1, 2, 3, 4, 5};
//         const size_t n = values.size();

//         // Push all values
//         if(!queue.PushMany(values)) {
//             std::cerr << "  ERROR: Failed to push value\n";
//             all_tests_passed = false;
//             goto test2_end;
//         }

//         // Pop all values and verify order
//         {
//             auto popped = queue.PopMany(n);
//             if (popped.empty()) {
//                 std::cerr << "  ERROR: Expected value but queue was empty\n";
//                 all_tests_passed = false;
//                 goto test2_end;
//             }
            
//             for (size_t i = 0; i < values.size(); ++i) {
//                 if (popped[i] != values[i]) {
//                     std::cerr << "  ERROR: Value " << i << " doesn't match\n";
//                     std::cerr << "  Expected: " << values[i] << "\n";
//                     std::cerr << "  Got:      " << popped[i] << "\n";
//                     all_tests_passed = false;
//                     goto test2_end;
//                 }
//             }
//         }

//         // Verify queue is empty
//         {
//             auto extra = queue.PopOne();
//             if (extra) {
//                 std::cerr << "  ERROR: Queue should be empty but returned a message\n";
//                 all_tests_passed = false;
//                 goto test2_end;
//             }
//         }

//         std::cout << "  PASS\n";
//         ++pass_count;
//         test2_end:;
//     }

//     // Test 3: Empty queue behavior
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Empty queue behavior\n";
//         SPSCFixedSize<int, 64> queue;

//         auto popped = queue.PopOne();
//         if (popped) {
//             std::cerr << "  ERROR: Pop on empty queue should return nullopt\n";
//             all_tests_passed = false;
//         } else {
//             std::cout << "  PASS\n";
//             ++pass_count;
//         }
//     }

//     // Test 4: Different message sizes
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Capacity limit (fixed size)\n";
//         SPSCFixedSize<int, 4> queue;  // Small capacity in elements

//         // Fill the queue
//         assert(queue.PushOne(1));
//         assert(queue.PushOne(2));
//         assert(queue.PushOne(3));
//         assert(queue.PushOne(4));

//         // Next push should fail because we treat N as element capacity
//         if (queue.PushOne(5)) {
//             std::cerr << "  ERROR: Push should have failed on full queue\n";
//             all_tests_passed = false;
//         } else {
//             std::cout << "  PASS\n";
//             ++pass_count;
//         }
//     }

//     // Test 5: Wrap-around scenario
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Wrap-around scenario\n";
//         SPSCFixedSize<int, 4> queue;  // Small capacity to force wrap-around

//         // Push values that will cause wrap-around via modulo indexing
//         std::vector<int> values1 = {10, 20, 30};
//         for (int v : values1) {
//             if (!queue.PushOne(v)) {
//                 std::cerr << "  ERROR: Failed to push value during wrap-around test\n";
//                 all_tests_passed = false;
//                 goto test5_wrap_end;
//             }
//         }

//         // Pop first value
//         {
//             auto popped1 = queue.PopOne();
//             if (!popped1 || popped1 != values1[0]) {
//                 std::cerr << "  ERROR: First value doesn't match after wrap-around\n";
//                 all_tests_passed = false;
//                 goto test5_wrap_end;
//             }
//         }

//         // Push another value (should wrap around)
//         {
//             int v4 = 40;
//             if (!queue.PushOne(v4)) {
//                 std::cerr << "  ERROR: Failed to push value after wrap-around\n";
//                 all_tests_passed = false;
//                 goto test5_wrap_end;
//             }

//             // Pop remaining values in order
//             std::vector<int> expected_tail = {values1[1], values1[2], v4};
//             for (size_t i = 0; i < expected_tail.size(); ++i) {
//                 auto popped2 = queue.PopOne();
//                 if (!popped2 || popped2 != expected_tail[i]) {
//                     std::cerr << "  ERROR: Value " << i << " doesn't match after wrap-around\n";
//                     all_tests_passed = false;
//                     goto test5_wrap_end;
//                 }
//             }
//         }

//         std::cout << "  PASS\n";
//         ++pass_count;
//         test5_wrap_end:;
//     }

//     // Test 6: Interleaved push and pop
//     {
//         ++test_count;
//         std::cout << "Test " << test_count << ": Interleaved push and pop\n";
//         SPSCFixedSize<int, 8> queue;

//         int v1 = 10;
//         int v2 = 20;
//         int v3 = 30;

//         // Push, pop, push, pop pattern
//         if (!queue.PushOne(v1)) {
//             std::cerr << "  ERROR: Failed to push v1\n";
//             all_tests_passed = false;
//             goto test6_end;
//         }

//         {
//             auto popped = queue.PopOne();
//             if (!popped || popped != v1) {
//                 std::cerr << "  ERROR: v1 doesn't match\n";
//                 all_tests_passed = false;
//                 goto test6_end;
//             }
//         }

//         if (!queue.PushOne(v2)) {
//             std::cerr << "  ERROR: Failed to push v2\n";
//             all_tests_passed = false;
//             goto test6_end;
//         }

//         if (!queue.PushOne(v3)) {
//             std::cerr << "  ERROR: Failed to push v3\n";
//             all_tests_passed = false;
//             goto test6_end;
//         }

//         {
//             auto popped2 = queue.PopOne();
//             if (!popped2 || popped2 != v2) {
//                 std::cerr << "  ERROR: v2 doesn't match\n";
//                 all_tests_passed = false;
//                 goto test6_end;
//             }
//         }

//         {
//             auto popped3 = queue.PopOne();
//             if (!popped3 || popped3 != v3) {
//                 std::cerr << "  ERROR: v3 doesn't match\n";
//                 all_tests_passed = false;
//                 goto test6_end;
//             }
//         }

//         std::cout << "  PASS\n";
//         ++pass_count;
//         test6_end:;
//     }

//     // Summary
//     std::cout << "\n";
//     std::cout << "========================================\n";
//     std::cout << "Test Summary: " << pass_count << "/" << test_count << " tests passed\n";
//     std::cout << "========================================\n";

//     if (all_tests_passed) {
//         std::cout << "SUCCESS: All tests passed!\n";
//         return 0;
//     } else {
//         std::cerr << "FAILURE: Some tests failed. See errors above.\n";
//         return 1;
//     }
// }