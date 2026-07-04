#pragma once
#include "../config.hpp"
#include <array>
#include <memory>
#include <cstdint>

using SMStateBytes = std::array<std::byte, SM_STATE_SIZE>;

struct Snapshot {
    std::unique_ptr<SMStateBytes> state; // local state could potentially be very large; must be stored on the heap
    uint32_t last_included_idx = 0;      // index of highest log entry applied to state machine
    uint32_t last_included_term = 0;     // term of the log entry at the last included index
};
