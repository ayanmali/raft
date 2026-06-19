#pragma once
#include <vector>
#include <cstddef>
#include <cstdint>

struct LogEntry {
    std::vector<std::byte> data;
    uint32_t term;
};
