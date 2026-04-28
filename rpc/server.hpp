#pragma once
#include "../node.hpp"
#include "./event_loop.hpp"
#include "../thread_pool/threadpool.hpp"
#include <cerrno>
#include <cstring>
#include <memory>
#include <netdb.h>
#include <signal.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace detail {

inline void bind_and_listen(int server_fd, const char* port) {
    addrinfo hints{};
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags    = AI_PASSIVE;

    addrinfo* res = nullptr;
    if (::getaddrinfo(nullptr, port, &hints, &res) != 0) {
        throw std::runtime_error("getaddrinfo failed");
    }
    // defers cleanup automatically
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> guard(res, &::freeaddrinfo);

    int yes = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    addrinfo* p = nullptr;
    for (p = res; p != nullptr; p = p->ai_next) {
        if (::bind(server_fd, p->ai_addr, p->ai_addrlen) == 0) break;
    }
    if (p == nullptr) throw std::runtime_error("bind failed");

    if (::listen(server_fd, SERVER_BACKLOG) != 0) {
        throw std::runtime_error("listen failed");
    }
}

} // namespace detail

inline void Node::server_expose(const char* port) {
    // SIGPIPE would otherwise kill the process if a peer disappears mid-send.
    // MSG_NOSIGNAL is also passed on every send() call as belt-and-suspenders.
    static const auto sigpipe_ignored = []{
        struct sigaction sa{};
        sa.sa_handler = SIG_IGN;
        sigemptyset(&sa.sa_mask);
        ::sigaction(SIGPIPE, &sa, nullptr);
        return true;
    }();
    (void)sigpipe_ignored;

    detail::bind_and_listen(server_fd, port);

    int evfd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (evfd < 0) throw std::runtime_error("eventfd failed");

    // The handler is stored by its real type inside ThreadPool, so the worker
    // call site is a direct call (no std::function indirection, no heap).
    // Capture-free lambda here; capture-by-value works the same way.
    auto handler = [](const std::byte* req, uint32_t len) -> OwnedBytes {
        auto* out = new std::byte[len];
        std::memcpy(out, req, len);
        return OwnedBytes{out, len};
    };
    using Pool = ThreadPool<decltype(handler)>;

    Pool                pool(evfd, handler);
    EventLoop<Pool>     loop(server_fd, evfd);
    loop.SetPool(&pool);

    pool.Start();
    try {
        loop.Run();
    } catch (...) {
        pool.Stop();
        ::close(evfd);
        throw;
    }
    pool.Stop();
    ::close(evfd);
}

