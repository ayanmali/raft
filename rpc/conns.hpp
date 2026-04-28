#include "../config.hpp"
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

// Pre-allocated pool of Connection slots with an intrusive freelist. Acquire
// pops the head; Release resets mutable state and pushes back. No per-accept
// allocation, no per-close deallocation.
struct ConnSlab {
    char*       buffer    = nullptr;
    Connection* free_head = nullptr;

    ConnSlab() {
        buffer = new char[sizeof(Connection) * MAX_CONNECTIONS];
        for (size_t i = 0; i < MAX_CONNECTIONS; ++i) {
            auto* c = ::new (buffer + i * sizeof(Connection)) Connection();
            c->next_free = free_head;
            free_head = c;
        }
    }
    ~ConnSlab() {
        for (size_t i = 0; i < MAX_CONNECTIONS; ++i) {
            reinterpret_cast<Connection*>(buffer + i * sizeof(Connection))
                ->~Connection();
        }
        delete[] buffer;
    }
    ConnSlab(const ConnSlab&)            = delete;
    ConnSlab& operator=(const ConnSlab&) = delete;

    Connection* Acquire() {
        if (!free_head) return nullptr;
        Connection* c = free_head;
        free_head = c->next_free;
        c->next_free = nullptr;
        return c;
    }
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
        c->next_free = free_head;
        free_head = c;
    }
};