// #include <stdexcept>
// struct socket_error : public std::runtime_error {

// };

// struct IdkErr2 : public std::runtime_error {

// };
#pragma once
#include <expected>

using VoidExpected = std::expected<void, const char*>;
inline auto Unexpected = [](const char* err_msg) { return std::unexpected(err_msg); };
