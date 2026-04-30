#pragma once
#include "../../node.hpp"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <netdb.h>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

/*
Client-side RPC dispatch.

client_request(ip, port, msg, len, out, out_cap) is the public entry point.
The flow is:

  1. pool.get(ip)
     - hit:   reuse the cached fd
     - miss:  socket() + connect(); pool.set(ip, fd)
  2. send_all(fd, [uint32_t len][msg]) and recv_frame(fd, out, out_cap, &n)
  3. on dead-connection error: close(fd), pool.remove(ip), retry once with
     a fresh fd; on second failure, throw.

Wire framing is symmetric with the server in rpc/event_loop.hpp:
  request:  [uint32_t len][len bytes payload]
  reply:    [uint32_t len][len bytes payload]
*/

namespace detail {

// Returns true if `err` (typically errno) indicates the underlying
// connection is dead and the caller should reconnect rather than retry on
// the same fd. EAGAIN/EWOULDBLOCK/EINTR are explicitly NOT dead — they
// mean "try again on the same fd".
inline bool is_dead_conn(int err) {
    switch (err) {
        case EPIPE:
        case ECONNRESET:
        case ECONNREFUSED:
        case ECONNABORTED:
        case ENOTCONN:
        case ETIMEDOUT:
        case EHOSTUNREACH:
        case ENETUNREACH:
        case ENETRESET:
        case EBADF:
            return true;
        default:
            return false;
    }
}

inline int connect_new(std::string_view ip, std::string_view port) {
    // string_view is not guaranteed null-terminated; getaddrinfo wants
    // C-strings, so copy onto the stack.
    char ip_buf[64];
    char port_buf[16];
    if (ip.size()   >= sizeof(ip_buf))   throw std::runtime_error("ip too long");
    if (port.size() >= sizeof(port_buf)) throw std::runtime_error("port too long");
    std::memcpy(ip_buf,   ip.data(),   ip.size());   ip_buf[ip.size()]     = '\0';
    std::memcpy(port_buf, port.data(), port.size()); port_buf[port.size()] = '\0';

    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_NUMERICHOST | AI_NUMERICSERV;

    addrinfo* res = nullptr;
    if (::getaddrinfo(ip_buf, port_buf, &hints, &res) != 0) {
        throw std::runtime_error("getaddrinfo failed");
    }

    int fd = -1;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) continue;
        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
        ::close(fd);
        fd = -1;
    }
    ::freeaddrinfo(res);

    if (fd < 0) throw std::runtime_error("connect failed");
    return fd;
}

// Returns true on full success. Returns false (and sets *dead=true) iff the
// connection died mid-send. Other errors throw.
inline bool send_all(int fd, const std::byte* buf, size_t n, bool* dead) {
    *dead = false;
    size_t sent = 0;
    while (sent < n) {
        ssize_t k = ::send(fd, buf + sent, n - sent, MSG_NOSIGNAL);
        if (k > 0) { sent += static_cast<size_t>(k); continue; }
        if (k < 0 && errno == EINTR) continue;
        if (k == 0) { *dead = true; return false; }
        if (is_dead_conn(errno)) { *dead = true; return false; }
        throw std::runtime_error("send failed");
    }
    return true;
}

// Returns true on full success. Returns false (and sets *dead=true) iff the
// connection died mid-recv. Other errors throw.
inline bool recv_exact(int fd, std::byte* buf, size_t n, bool* dead) {
    *dead = false;
    size_t got = 0;
    while (got < n) {
        ssize_t k = ::recv(fd, buf + got, n - got, 0);
        if (k > 0) { got += static_cast<size_t>(k); continue; }
        if (k < 0 && errno == EINTR) continue;
        if (k == 0) { *dead = true; return false; }
        if (is_dead_conn(errno)) { *dead = true; return false; }
        throw std::runtime_error("recv failed");
    }
    return true;
}

} // namespace detail

// inline: defined in a header included by multiple TUs (rpc.hpp, server.hpp).
inline uint32_t Node::client_request(std::string_view ip,
                                     std::string_view port,
                                     const std::byte* msg, uint32_t len,
                                     std::byte* out, uint32_t out_cap) {
    // Two attempts: one with whatever the cache has (or a fresh fd on
    // miss), one with a fresh fd if the first failed mid-RPC.
    for (int attempt = 0; attempt < 2; ++attempt) {
        int fd = connections.get(ip);
        if (fd < 0) {
            fd = detail::connect_new(ip, port);
            int evicted = -1;
            connections.set(ip, fd, &evicted);
            if (evicted >= 0) ::close(evicted);
        }

        bool dead = false;

        // Send header + payload as two writes. send_all handles partial
        // writes; the kernel will coalesce on the wire when possible.
        const uint32_t hdr = len;
        if (detail::send_all(fd, reinterpret_cast<const std::byte*>(&hdr),
                             sizeof(hdr), &dead) &&
            (len == 0 || detail::send_all(fd, msg, len, &dead))) {
            uint32_t reply_len = 0;
            if (detail::recv_exact(fd, reinterpret_cast<std::byte*>(&reply_len),
                                   sizeof(reply_len), &dead)) {
                if (reply_len > out_cap) {
                    // Reply is larger than caller's buffer. Drop the
                    // connection (we can't recover sync) and surface.
                    connections.remove(ip);
                    ::close(fd);
                    throw std::runtime_error("reply exceeds out_cap");
                }
                if (reply_len == 0 ||
                    detail::recv_exact(fd, out, reply_len, &dead)) {
                    return reply_len;
                }
            }
        }

        if (!dead) {
            // Threw above on non-dead errors; reaching here with dead=false
            // shouldn't happen, but stay defensive.
            connections.remove(ip);
            ::close(fd);
            throw std::runtime_error("rpc failed");
        }

        // Connection died mid-RPC. Drop it and retry once.
        connections.remove(ip);
        ::close(fd);
    }
    throw std::runtime_error("peer unreachable after retry");
}
