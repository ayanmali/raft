#pragma once
#include "../slab.hpp"
#include <vector>

struct Connection {
    int      fd            = -1;
    uint64_t id            = 0;

    std::vector<std::byte> rbuf;
    std::vector<std::byte> wbuf;
    size_t   wbuf_offset   = 0;

    int      pending_tasks = 0;
    bool     closing       = false;
    uint32_t epoll_events  = 0;
    uint32_t next_seq      = 0;

    Connection* next_free  = nullptr;   // freelist link, valid only when free
};

// Thin wrapper over Slab<Connection> that preserves the per-Connection
// reset semantics on Release. The slab itself is reset-agnostic; the
// std::vector clears + scalar zeroing live here because they're specific
// to Connection.
struct ConnSlab {
    Slab<Connection> slab;

    ConnSlab(size_t max_server_conns) : slab(max_server_conns) {}

    ConnSlab(const ConnSlab&)            = delete;
    ConnSlab& operator=(const ConnSlab&) = delete;

    Connection* Acquire() { return slab.Acquire(); }

    void Release(Connection* c) {
        c->fd = -1;
        c->id = 0;
        c->rbuf.clear();
        c->rbuf.shrink_to_fit();
        c->wbuf.clear();
        c->wbuf.shrink_to_fit();
        c->wbuf_offset = 0;
        c->pending_tasks = 0;
        c->closing = false;
        c->epoll_events = 0;
        c->next_seq = 0;
        slab.Release(c);
    }
};
