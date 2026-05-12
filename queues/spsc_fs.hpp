// #include <atomic>
// #include <cstddef>
// #include <cstring>
// #include <vector>
// #include "./utils.hpp"
// #include "../config.hpp"

// /*
// Single-producer single-consumer queue that stores variable length byte payloads.
// Each message is laid out as: [size_t payload_size][payload bytes...].
// The read/write counters grow monotonically; indices into the buffer are derived
// with modulo arithmetic so wrap-around is handled transparently.

// Each write (push) moves the write_idx up by one
// Each read (pop) moves the read_idx up by one

// Variable-length message size

// Template parameter N = size of queue
// N should be a power of 2
// */
// template <size_t N>
// struct SPSC {
//     alignas(CACHE_LINE_SIZE) std::atomic_size_t read_idx{0};   // owned by consumer
//     alignas(CACHE_LINE_SIZE) std::atomic_size_t write_idx{0};  // owned by producer
//     uint8_t buffer[N];

//     // Returns true on success, false if there is not enough room.
//     //
//     // The caller precomputes `payload_size` (sum of bytes across all
//     // serialized fields) so we can bounds-check up front. `write_fn` is
//     // invoked exactly once with (buffer, N, off) where `off` is the
//     // logical write offset just past the size header; the lambda is
//     // expected to write exactly `payload_size` bytes via Queues::CopyIn
//     // so wrap-around is handled.
//     template <typename WriteFn>
//     bool Push(size_t payload_size, WriteFn&& write_fn) {
//         const size_t write = write_idx.load(std::memory_order_relaxed);
//         const size_t read  = read_idx.load(std::memory_order_acquire);

//         const size_t total_size = HEADER_SIZE + payload_size;
//         if ((write - read) + total_size > N) return false;

//         const size_t offset = write & (N - 1);
//         Queues::CopyIn(buffer, N, offset, &payload_size, HEADER_SIZE);
//         write_fn(buffer, N, offset + HEADER_SIZE);

//         write_idx.store(write + total_size, std::memory_order_release);
//         return true;
//     }

//     // Returns false if there is no message available.
//     bool Pop(std::vector<std::byte>& payload) {
//         const size_t read = read_idx.load(std::memory_order_relaxed);
//         const size_t write = write_idx.load(std::memory_order_acquire);
//         if (read == write) return false;

//         const size_t offset = read & (N - 1);
//         size_t payload_size = 0;
//         Queues::CopyOut(buffer, N, offset, &payload_size, HEADER_SIZE);

//         const size_t total_size = HEADER_SIZE + payload_size;
//         if (read + total_size > write) return false;  // incomplete write

//         //std::vector<std::byte> payload(payload_size);
//         if (payload_size > 0) {
//             Queues::CopyOut(buffer, N, offset + HEADER_SIZE, payload.data(), payload_size);
//         }

//         read_idx.store(read + total_size, std::memory_order_release);
//         return true;
//     }
// };

// // int main() {
// //     SPSC<64> queue;

// //     std::array<std::byte, 5> message{
// //         std::byte{0x48}, std::byte{0x65}, std::byte{0x6c},
// //         std::byte{0x6c}, std::byte{0x6f},
// //     };

// //     if (!queue.Push(message)) {
// //         std::cerr << "Failed to enqueue message\n";
// //         return 1;
// //     }

// //     std::vector<std::byte> popped = queue.Pop();
// //     if (popped.empty()) {
// //         std::cerr << "Queue unexpectedly empty\n";
// //         return 1;
// //     }

// //     std::cout << "Read " << popped.size() << " bytes: ";
// //     for (std::byte b : popped) {
// //         std::cout << std::to_integer<int>(b) << ' ';
// //     }
// //     std::cout << '\n';
// //     return 0;
// // }