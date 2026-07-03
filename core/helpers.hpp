#include <cstddef>
#include <cstdint>

template <size_t N>
struct ByteArray { std::byte bytes[N]; };

inline int8_t bytes_to_int8(const std::byte* bytes) {
    uint8_t u = std::to_integer<uint8_t>(bytes[0]);
    return static_cast<int8_t>(u);
}

inline int16_t bytes_to_int16(const std::byte* bytes) {
    uint16_t u =
        std::to_integer<uint16_t>(bytes[0]) |
        (std::to_integer<uint16_t>(bytes[1]) << 8);

    return static_cast<int16_t>(u);
}

inline int32_t bytes_to_int32(const std::byte* bytes) {
    uint32_t u =
        std::to_integer<uint32_t>(bytes[0]) |
        (std::to_integer<uint32_t>(bytes[1]) << 8) |
        (std::to_integer<uint32_t>(bytes[2]) << 16) |
        (std::to_integer<uint32_t>(bytes[3]) << 24);

    return static_cast<int32_t>(u);
}

inline int64_t bytes_to_int64(const std::byte* bytes) {
    uint64_t u =
        std::to_integer<uint64_t>(bytes[0]) |
        (std::to_integer<uint64_t>(bytes[1]) << 8) |
        (std::to_integer<uint64_t>(bytes[2]) << 16) |
        (std::to_integer<uint64_t>(bytes[3]) << 24) |
        (std::to_integer<uint64_t>(bytes[4]) << 32) |
        (std::to_integer<uint64_t>(bytes[5]) << 40) |
        (std::to_integer<uint64_t>(bytes[6]) << 48) |
        (std::to_integer<uint64_t>(bytes[7]) << 56);

    return static_cast<int64_t>(u);
}
