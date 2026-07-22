// To use when supporting C++23
// #pragma once
// #include <expected>
// #include <optional>
// #include <string>

// using VoidExpected = std::expected<void, const char*>;
// using std::optional<std::string> = std::expected<void, std::string>;
//
// inline auto Unexpected = [](const char* err_msg) { return std::unexpected(err_msg); };
// inline auto UnexpectedF = [](const std::string& err_msg) { return std::unexpected(err_msg); };
