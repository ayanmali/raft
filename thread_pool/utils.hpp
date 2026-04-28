#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>

static constexpr size_t HEADER_SIZE = sizeof(size_t);

// Copy helpers that handle wrap-around for a fixed-size byte ring buffer.
template <typename T>
inline void CopyIn(T* buffer, size_t capacity, size_t offset, const void* src, size_t len) {
    const T* src_bytes = static_cast<const T*>(src);
    size_t start = offset & (capacity - 1);
    size_t first = std::min(len, capacity - start);
    std::memcpy(buffer + start, src_bytes, first);
    if (len > first) {
        std::memcpy(buffer, src_bytes + first, len - first);
    }
}

template <typename T>
inline void CopyOut(const T* buffer, size_t capacity, size_t offset, void* dst, size_t len) {
    T* dst_bytes = static_cast<T*>(dst);
    size_t start = offset & (capacity - 1);
    size_t first = std::min(len, capacity - start);
    std::memcpy(dst_bytes, buffer + start, first);
    if (len > first) {
        std::memcpy(dst_bytes + first, buffer, len - first);
    }
}