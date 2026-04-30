#pragma once
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <new>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "../../config.hpp"
#include "../../thread_pool/threadpool.hpp"
#include "./conns.hpp"

/*
Length-prefixed framing on the wire: [uint32_t len][len bytes payload].
Length is host-order; symmetric on both sides.

Per-connection state machine:
- rbuf accumulates raw bytes from recv().
- The loop attempts to extract one complete request and dispatches it as a
  Task. While pending_tasks > 0, no further requests from this connection are
  parsed (one in-flight per connection => no out-of-order responses).
- On completion, response bytes are appended to wbuf and EPOLLOUT is armed.
- On EPOLLOUT, wbuf is drained; once empty, EPOLLOUT is disarmed so epoll
  doesn't keep waking the loop on a writable kernel send buffer.

Lifetime:
- Each accepted connection gets a monotonically increasing conn_id. Tasks
  and Completions reference conn_id (not fd), so a closed-and-reused fd
  cannot mis-deliver a stale completion.
- Connection objects are slab-allocated up front (ConnSlab). The conns map
  holds non-owning pointers; the slab owns the underlying storage.
- pending_tasks acts as a refcount. Connections with closing=true linger in
  conns until all in-flight worker tasks return and their completions are
  drained (and their bytes freed). Then they're released back to the slab.
*/

inline constexpr size_t FRAME_HEADER_SIZE = sizeof(uint32_t);
inline constexpr size_t RECV_CHUNK        = 4096;
inline constexpr int    EPOLL_BATCH       = 64;


inline void set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0) throw std::runtime_error("fcntl F_GETFL failed");
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        throw std::runtime_error("fcntl F_SETFL O_NONBLOCK failed");
    }
}

// EventLoop is templated on the concrete ThreadPool type so the calls into
// the pool stay direct (no virtual dispatch). The pool exposes its tasks
// (SPMCQueue) and DrainCompletions; both are non-virtual and inlinable.
template <typename Pool>
struct EventLoop {
    int  epoll_fd  = -1;
    int  listen_fd = -1;
    int  event_fd  = -1;
    std::atomic<bool> stopped{false};

    ConnSlab                                slab;
    std::unordered_map<uint64_t, Connection*> conns;
    std::unordered_map<int, uint64_t>       fd_to_id;
    std::unordered_set<uint64_t>            stalled; // backpressured

    uint64_t next_conn_id = 1;
    Pool*    pool         = nullptr;

    EventLoop(int listen_fd_, int event_fd_)
        : listen_fd(listen_fd_), event_fd(event_fd_) {
        epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd < 0) throw std::runtime_error("epoll_create1 failed");

        set_nonblocking(listen_fd);
        register_fd(listen_fd, EPOLLIN);
        register_fd(event_fd,  EPOLLIN);
    }

    ~EventLoop() {
        if (epoll_fd >= 0) ::close(epoll_fd);
    }

    EventLoop(const EventLoop&)            = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void SetPool(Pool* p) { pool = p; }
    void Stop()           { stopped.store(true, std::memory_order_release); }

    void Run() {
        epoll_event evs[EPOLL_BATCH];
        while (!stopped.load(std::memory_order_acquire)) {
            int n = ::epoll_wait(epoll_fd, evs, EPOLL_BATCH, -1);
            if (n < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error("epoll_wait failed");
            }
            for (int i = 0; i < n; ++i) {
                const int fd = evs[i].data.fd;
                const uint32_t e = evs[i].events;

                if (fd == listen_fd) { Accept(); continue; }
                if (fd == event_fd)  { OnEventfd(); continue; }

                auto it = fd_to_id.find(fd);
                if (it == fd_to_id.end()) continue;
                auto cit = conns.find(it->second);
                if (cit == conns.end()) continue;
                Connection& c = *cit->second;

                if (e & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                    CloseConn(c);
                    continue;
                }
                if (e & EPOLLIN)  OnReadable(c);
                if (e & EPOLLOUT) OnWritable(c);
            }
        }
    }

private:
    void register_fd(int fd, uint32_t events) {
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            throw std::runtime_error("epoll_ctl ADD failed");
        }
    }

    void modify_interest(Connection& c, uint32_t events) {
        if (c.epoll_events == events) return;
        epoll_event ev{};
        ev.events  = events;
        ev.data.fd = c.fd;
        if (::epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c.fd, &ev) < 0) {
            // If MOD fails the fd is in a bad state; close it.
            CloseConn(c);
            return;
        }
        c.epoll_events = events;
    }

    void Accept() {
        for (;;) {
            sockaddr_in peer{};
            socklen_t   plen = sizeof(peer);
            int fd = ::accept4(listen_fd, reinterpret_cast<sockaddr*>(&peer),
                               &plen, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                return; // transient errors: drop and try again on next epoll wake
            }

            Connection* c = slab.Acquire();
            if (!c) {
                // Hard cap: refuse new connections rather than allocate.
                ::close(fd);
                continue;
            }
            c->fd = fd;
            c->id = next_conn_id++;
            c->epoll_events = EPOLLIN | EPOLLRDHUP;

            epoll_event ev{};
            ev.events  = c->epoll_events;
            ev.data.fd = fd;
            if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
                ::close(fd);
                slab.Release(c);
                continue;
            }
            fd_to_id[fd] = c->id;
            conns.emplace(c->id, c);
        }
    }

    void OnReadable(Connection& c) {
        std::byte tmp[RECV_CHUNK];
        for (;;) {
            ssize_t n = ::recv(c.fd, tmp, sizeof(tmp), 0);
            if (n > 0) {
                c.rbuf.insert(c.rbuf.end(), tmp, tmp + n);
                continue;
            }
            if (n == 0) { CloseConn(c); return; }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            CloseConn(c);
            return;
        }
        TryDispatch(c);
    }

    // Attempts to extract one framed request and submit it as a Task by
    // writing directly into the SPMC queue's slot. Returns true on success.
    bool TryDispatch(Connection& c) {
        if (c.closing || c.pending_tasks > 0) return false;
        if (c.rbuf.size() < FRAME_HEADER_SIZE) return false;

        uint32_t len = 0;
        std::memcpy(&len, c.rbuf.data(), FRAME_HEADER_SIZE);
        if (c.rbuf.size() < FRAME_HEADER_SIZE + len) return false;

        Task* slot = pool->tasks.AcquireWriteSlot();
        if (!slot) {
            stalled.insert(c.id);
            modify_interest(c, c.epoll_events & ~EPOLLIN);
            return false;
        }

        slot->conn_id = c.id;
        slot->seq     = c.next_seq;
        slot->len     = len;
        const std::byte* src = c.rbuf.data() + FRAME_HEADER_SIZE;
        if (len <= INLINE_PAYLOAD_BYTES) {
            slot->heap_data = nullptr;
            std::memcpy(slot->inline_data, src, len);
        } else {
            slot->heap_data = new std::byte[len];
            std::memcpy(slot->heap_data, src, len);
        }
        pool->tasks.CommitWrite();

        ++c.next_seq;
        c.rbuf.erase(c.rbuf.begin(), c.rbuf.begin() + FRAME_HEADER_SIZE + len);
        ++c.pending_tasks;
        return true;
    }

    void OnWritable(Connection& c) {
        while (c.wbuf_offset < c.wbuf.size()) {
            ssize_t n = ::send(c.fd,
                               c.wbuf.data() + c.wbuf_offset,
                               c.wbuf.size() - c.wbuf_offset,
                               MSG_NOSIGNAL);
            if (n > 0) { c.wbuf_offset += static_cast<size_t>(n); continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
            CloseConn(c);
            return;
        }
        c.wbuf.clear();
        c.wbuf_offset = 0;
        modify_interest(c, c.epoll_events & ~EPOLLOUT);
        if (c.closing && c.pending_tasks == 0) ReapClosed(c);
    }

    void OnEventfd() {
        uint64_t counter;
        for (;;) {
            ssize_t n = ::read(event_fd, &counter, sizeof(counter));
            if (n == sizeof(counter)) break;
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
            break;
        }

        // Drain *all* sub-queues regardless of counter — workers may have
        // pushed without bumping the counter (or coalesced multiple wakes).
        pool->DrainCompletions([this](Completion& comp) {
            const std::byte* data = payload(comp);
            auto cit = conns.find(comp.conn_id);

            if (cit == conns.end()) {
                if (comp.heap_data) delete[] comp.heap_data;
                return;
            }
            Connection& c = *cit->second;

            if (c.closing) {
                if (comp.heap_data) delete[] comp.heap_data;
                --c.pending_tasks;
                if (c.pending_tasks == 0 && c.wbuf.empty()) ReapClosed(c);
                return;
            }

            c.wbuf.insert(c.wbuf.end(), data, data + comp.len);
            if (comp.heap_data) delete[] comp.heap_data;
            --c.pending_tasks;

            modify_interest(c, c.epoll_events | EPOLLOUT);
        });

        // Slots may have freed up. Retry stalled connections; if dispatch
        // succeeds, re-arm EPOLLIN. Snapshot first to avoid iterator
        // invalidation if TryDispatch closes a connection.
        std::vector<uint64_t> retry(stalled.begin(), stalled.end());
        for (uint64_t id : retry) {
            auto cit = conns.find(id);
            if (cit == conns.end()) { stalled.erase(id); continue; }
            Connection& c = *cit->second;
            modify_interest(c, c.epoll_events | EPOLLIN);
            if (TryDispatch(c)) stalled.erase(id);
        }

        // Dispatch the next request for any connection whose pending count
        // just dropped to zero and still has buffered bytes.
        std::vector<uint64_t> dispatchable;
        dispatchable.reserve(conns.size());
        for (auto& [id, cptr] : conns) {
            if (!cptr->closing && cptr->pending_tasks == 0 && !cptr->rbuf.empty()) {
                dispatchable.push_back(id);
            }
        }
        for (uint64_t id : dispatchable) {
            auto cit = conns.find(id);
            if (cit != conns.end()) TryDispatch(*cit->second);
        }
    }

    void CloseConn(Connection& c) {
        if (c.closing) return;
        c.closing = true;

        ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c.fd, nullptr);
        ::close(c.fd);
        fd_to_id.erase(c.fd);
        c.fd = -1;
        c.epoll_events = 0;

        stalled.erase(c.id);

        if (c.pending_tasks == 0) ReapClosed(c);
    }

    void ReapClosed(Connection& c) {
        const uint64_t id = c.id;
        Connection* ptr   = &c;
        conns.erase(id);
        slab.Release(ptr);
    }
};
