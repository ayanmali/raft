#pragma once
#include <algorithm>
#include <span>
#include <unordered_map>
#include <vector>
#include <cstddef>
#include <cstdint>

struct LogEntry {
    std::vector<std::byte> data;
    uint32_t term;
};

inline void commit_if_quorum(std::span<int> match_index, uint32_t& commit_index, const uint32_t& current_term, std::span<LogEntry> log) {
    auto freqs = std::unordered_map<uint32_t, uint32_t>(match_index.size());
    for (auto match_idx : match_index) {
        freqs[match_idx]++;
    }

    // fast path
    auto kv_max_freq = std::max_element(freqs.begin(), freqs.end(), [](const std::pair<uint32_t, uint32_t>& a, const std::pair<uint32_t, uint32_t>& b){
       return a.second > b.second;
    });
    if (kv_max_freq->second <= match_index.size() / 2) return;
    if (kv_max_freq->first > commit_index && log[kv_max_freq->first].term == current_term) {
        commit_index = kv_max_freq->first;
        return;
    }

    // slow path
    freqs.erase(kv_max_freq);
    std::vector<std::pair<uint32_t, uint32_t>> freqs_vec(freqs.size());
    for (auto [idx, count] : freqs) {
        freqs_vec.emplace_back(idx, count);
    }
    std::sort(freqs_vec.begin(), freqs_vec.end(), [](auto& a, auto& b){ return a.second > b.second; });

    for (auto& [match_idx, count] : freqs_vec) {
        if (count < match_index.size() / 2) break;
        if (match_idx > commit_index && log[match_idx].term == current_term) {
            commit_index = match_idx;
            break;
        }
    }
}
