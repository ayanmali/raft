#pragma once
/*
Connection state for inbound (server-side) and outbound (client-side, per-peer)
connections. Both flavors live in a per-thread EventLoop.

Inbound conns are slab-allocated for O(1) acquire/release and a hard cap on
concurrent client connections (MAX_SERVER_CONNS). Outbound peer conns are
held in a small unordered_map keyed by NodeID -- peer count is bounded by
cluster size, so a slab there would be overkill.

Buffering convention (used by both flavors):
  - rbuf  : raw bytes pulled from recv(), parsed front-to-back. The parser
            offset is implicit (bytes are erased from the front when a frame
            is consumed).
  - wbuf  : pending bytes to send(). wbuf_offset points at the next byte to
            send. Once wbuf_offset == wbuf.size(), the buffer is reset and
            EPOLLOUT is disarmed.
*/
#include "./protocol/payloads.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/types.h>
#include <type_traits>
#include <utility>
#include <vector>
#include <algorithm>

enum class RpcKind : uint8_t { AppendEntries, RequestVote, InstallSnapshot };
constexpr uint32_t RESP_SIZE = static_cast<uint32_t>(
    std::max(
        {
            AppendEntriesRespPayload::size(),
            RequestVoteRespPayload::size(),
            InstallSnapshotRespPayload::size()
        }
    )
);

struct ClientConn {
    std::vector<std::byte> rbuf;
    std::byte wbuf[RESP_SIZE + sizeof(RESP_SIZE) + sizeof(RpcKind)];

    char client_ip_addr[INET_ADDRSTRLEN];

    ClientConn* next_free  = nullptr; // freelist link, valid only when free

    size_t wbuf_offset     =  0; // to track how much of the wbuf has been sent (for chunked sends)
    size_t wbuf_size       =  0; // tracks the number of serialized bytes in wbuf to send over the network
    FD fd                  = -1;
    //ClientID client_id     =  0;
    // NodeID id;

    // Reserved: when request handlers run on a worker pool, this counts
    // outstanding tasks for the connection so we can defer reaping a
    // closed conn until all completions land. With synchronous Raft
    // // handlers (current state) this stays 0.
    //int      pending_tasks = 0;

    // bool     want_write    = false;
    uint32_t epoll_events  =  0;
    bool     closing       =  false;
    // uint32_t next_seq      = 0;

};

/*
Slab<T>: pre-allocated, fixed-capacity storage with an intrusive
singly-linked freelist for O(1) acquire/release.

Contract on T:
  - T must be default-constructible.
  - T must expose a public member `T* next_free`. While the slot is on the
    freelist, `next_free` points to the next free slot (or nullptr at the
    tail). While the slot is "in use" the field is unused; convention is
    to leave it as nullptr.

Lifetime:
  - The ctor placement-new's `cap` instances of T into one heap-allocated
    byte buffer and links them all onto the freelist.
  - Acquire() pops the freelist head; Release(t) pushes onto the freelist.
  - Release() does NOT reset T's user-visible state; the caller is
    responsible for that immediately before calling Release(). This keeps
    Slab<T> agnostic to the per-T reset semantics (e.g. clearing
    std::vector buffers vs zeroing scalar fields).
  - The dtor walks every slot by buffer index calling ~T(), because the
    freelist and "in use" sets are interleaved through the buffer and
    cannot be enumerated from either chain alone.

Not thread-safe. Single-owner.
*/
template <typename T>
struct Slab {
    static_assert(std::is_same_v<decltype(std::declval<T&>().next_free), T*>,
                  "Slab<T> requires a public member `T* next_free`");

    explicit Slab(std::size_t cap) : cap_{cap} {
        buffer_ = new char[sizeof(T) * cap_];
        for (std::size_t i = 0; i < cap_; ++i) {
            auto* n = ::new (buffer_ + i * sizeof(T)) T();
            n->next_free = free_head_;
            free_head_   = n;
        }
    }

    ~Slab() {
        for (std::size_t i = 0; i < cap_; ++i) {
            slot_at(i)->~T();
        }
        delete[] buffer_;
        buffer_ = nullptr;
    }

    Slab(const Slab&)            = delete;
    Slab& operator=(const Slab&) = delete;

    T* Acquire() {
        if (!free_head_) return nullptr;
        T* n = free_head_;
        free_head_   = n->next_free;
        n->next_free = nullptr;
        return n;
    }

    void Release(T* n) {
        n->next_free = free_head_;
        free_head_   = n;
    }

    std::size_t capacity() const { return cap_; }

    // Visits every slot by index, regardless of whether it is currently on
    // the freelist or in use. Useful for whole-pool teardown that needs to
    // touch all instances (callers that maintain their own "in use" set
    // should iterate that set instead).
    template <typename F>
    void ForEachSlot(F&& fn) {
        for (std::size_t i = 0; i < cap_; ++i) {
            fn(*slot_at(i));
        }
    }

private:
    T* slot_at(std::size_t i) {
        return reinterpret_cast<T*>(buffer_ + i * sizeof(T));
    }

    char*       buffer_    = nullptr;
    T*          free_head_ = nullptr;
    std::size_t cap_       = 0;
};

struct ClientConnSlab {
    Slab<ClientConn> slab;

    explicit ClientConnSlab(size_t max_server_conns) : slab(max_server_conns) {}

    ClientConnSlab(const ClientConnSlab&)            = delete;
    ClientConnSlab& operator=(const ClientConnSlab&) = delete;

    ClientConn* Acquire() { return slab.Acquire(); }

    void Release(ClientConn* c) {
        c->rbuf.clear();
        c->rbuf.shrink_to_fit();
        std::memset(c->wbuf, 0, sizeof(c->wbuf));
        c->wbuf_offset = 0;
        c->wbuf_size = 0;
        std::memset(c->client_ip_addr, 0, sizeof(c->client_ip_addr));
        c->fd = -1;
        //c->client_id = 0;
        // c->id = 0;
        //c->pending_tasks = 0;
        c->closing = false;
        // c->want_write = false;
        c->epoll_events = 0;
        // c->next_seq = 0;
        slab.Release(c);
    }
};

/*
Per-peer outbound connection state. PeerConn lives in the EventLoop that
owns this peer (peer_id % N); no cross-thread access.

State machine:
  Disconnected -> Connecting -> Connected -> Disconnected (on EOF/error)

  Disconnected: fd == -1. Pending writes queue up in `wbuf`; the loop
                kicks a non-blocking connect() before draining.
  Connecting:   fd >= 0, EPOLLOUT armed. Connection completion arrives
                as EPOLLOUT with SO_ERROR == 0.
  Connected:    EPOLLIN always armed; EPOLLOUT armed iff wbuf_offset <
                wbuf.size().

Buffering (mirrors ClientConn):
  - wbuf: outbound bytes. Serialized requests are appended; chunked sends
          advance wbuf_offset. The buffer is cleared after all bytes are
          transmitted and EPOLLOUT is disarmed.
  - rbuf: inbound reply bytes. Replies carry a 4-byte length prefix
          followed by a 1-byte RpcKind and the payload, so the parser is
          self-describing — no per-request tracking needed.
*/
struct PeerConn {
    // Single write buffer for all outbound data (requests are serialized
    // and appended). wbuf_offset tracks chunked-send progress.
    std::vector<std::byte> wbuf;

    std::byte rbuf_[RESP_SIZE + sizeof(RESP_SIZE) + sizeof(RpcKind)];

    // Configuration (set once when the peer subset is wired into the loop).
    const char* ip        = nullptr;
    const char* port      = nullptr;

    size_t wbuf_offset    = 0;
    size_t rbuf_offset    = 0;

    NodeID      peer_id   = 0;

    FD    fd              = -1;
    FD timer_fd           = -1;

    uint32_t epoll_events = 0;

    // Connection state.
    enum class State : uint8_t { Disconnected, Connecting, Connected };
    State state           = State::Disconnected;

    PeerConn(const char* ip_, const char* port_, NodeID peer_id_) : ip{ip_}, port{port_}, peer_id{peer_id_} {}
};
