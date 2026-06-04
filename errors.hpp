#pragma once
#include <expected>
#include <string>

using VoidExpected = std::expected<void, const char*>;
using VoidExpectedF = std::expected<void, std::string>;
inline auto Unexpected = [](const char* err_msg) { return std::unexpected(err_msg); };
inline auto UnexpectedF = [](const std::string& err_msg) { return std::unexpected(err_msg); };
