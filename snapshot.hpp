#pragma once
#include "config.hpp"
#include <cstddef>
#include <cstdint>

struct Snapshot {
    std::byte state[SM_STATE_SIZE];
    uint32_t last_included_idx = 0;    // index of highest log entry applied to state machine
    uint32_t last_included_term = 0;    // term of the log entry at the last included index
};
