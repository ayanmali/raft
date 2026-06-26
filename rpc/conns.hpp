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
#include <deque>
#include <sys/types.h>
#include <type_traits>
#include <utility>
#include <vector>

using NodeID = uint64_t;
using ClientID = uint64_t;
using FD = int;

// Peer endpoints in stable storage. Indexed by NodeID.
struct PeerInfo {
    NodeID      id;
    const char* ip;
    const char* port;
};

// Tracks the kind of in-flight RPC for a given peer connection so the
// reply parser knows which deserialize_*_resp to run. Replies do not
// carry an id byte (see protocol.hpp), so the caller maintains its own
// FIFO of pending request kinds and matches them to incoming replies.
enum class RpcKind : uint8_t { AppendEntries, RequestVote, InstallSnapshot };

// Messages that are sent from Raft layer to an event loop to send RPCs/replies to peers/clients.
struct RaftMessage {
    RpcMessage data;
    // std::vector<std::byte> data; // serialized bytes
    // PendingReply           reply;
    // F on_reply;

    // // only used for ArmTimer messages
    // std::chrono::nanoseconds period{0};
    // RpcKind kind;
    // Routing key (target loop = peer_id % N). Always meaningful.
    NodeID node_id = 0;
};

// One slot in a peer's in-flight queue. Exactly one of `on_ae`/`on_rv`/
// `on_is` is populated based on `kind`. The EventLoop pops the head of the
// queue when a complete reply arrives and invokes the matching callback.
// template <typename AEF, typename RVF, typename ISF>
// struct PendingReply {
//     RpcKind kind;
//     /* One of these functions will be called when the response comes back from the peer */

//     std::function<void(AppendEntriesRespPayload)>   on_ae;
//     std::function<void(RequestVoteRespPayload)>     on_rv;
//     std::function<void(InstallSnapshotRespPayload)> on_is;
//     // AEF on_ae;
//     // RVF on_rv;
//     // ISF on_is;
// };

struct ClientConn {
    std::vector<std::byte> rbuf;
    std::vector<std::byte> wbuf;
    IPAddress client_ip_addr;
    size_t wbuf_offset     =  0; // to track how much of the wbuf has been sent (for chunked sends)

    ClientConn* next_free  = nullptr; // freelist link, valid only when free

    FD fd                  = -1;
    //ClientID client_id     =  0;
    // NodeID cluster_id      =  0;

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
        c->wbuf.clear();
        c->wbuf.shrink_to_fit();
        c->wbuf_offset = 0;
        std::memset(c->client_ip_addr, 0, sizeof(c->client_ip_addr));
        c->fd = -1;
        //c->client_id = 0;
        // c->cluster_id = 0;
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

  Disconnected: fd == -1. Pending sends queue up in `wbuf`; the loop
                kicks a non-blocking connect() before draining.
  Connecting:   fd >= 0, EPOLLOUT armed. Connection completion arrives
                as EPOLLOUT with SO_ERROR == 0.
  Connected:    EPOLLIN always armed; EPOLLOUT armed iff wbuf_offset <
                wbuf.size().

In-flight tracking:
  Raft RPCs are synchronous request/reply over a single peer connection,
  so replies arrive in send order. `inflight` records the kind of each
  outstanding request so OnPeerReadable knows which deserialize_*_resp
  to invoke. `inflight_handlers` carries the response callbacks in the
  same order.
*/
struct InflightRPC {
    std::vector<std::byte> req;
    std::vector<std::byte> reply;
    size_t bytes_sent; // for chunked sends
    RpcKind kind;
};

struct PeerConn {
    enum class State : uint8_t { Disconnected, Connecting, Connected };

    // Configuration (set once when the peer subset is wired into the loop).
    const char* ip   = nullptr;
    const char* port = nullptr;
    NodeID      peer_id = 0;

    // Connection state.
    State state = State::Disconnected;
    FD    fd    = -1;
    uint32_t epoll_events = 0;

    // Buffering, mirroring ClientConn's layout.
    // std::vector<std::byte> rbuf;
    // std::vector<std::byte> wbuf;

    // Reply matching: pending replies in send order. The head is the
    // currently-arriving reply. Entry includes both the kind (drives the
    // deserializer) and the user callback to invoke on completion.
    std::deque<InflightRPC> outbox;

    FD timer_fd = -1;
    //std::chrono::steady_clock::time_point next_due; // for diagnostics
    //uint64_t in_flight_seq = 0; // matches up timer fires w/ the AE sent

    PeerConn(const char* ip_, const char* port_, NodeID peer_id_) : ip{ip_}, port{port_}, peer_id{peer_id_} {}
};
