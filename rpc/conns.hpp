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
#include "../config.hpp"
#include "./protocol/payloads.hpp"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sys/types.h>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <algorithm>

constexpr SocketType TCP = SocketType::TCP;
constexpr SocketType UDP = SocketType::UDP;
enum class RpcKind : uint8_t { AppendEntries, RequestVote, InstallSnapshot, ForwardLeader };

constexpr size_t REQ_SIZE = std::max(
    {

        AppendEntriesReqPayload::size(),
        RequestVoteReqPayload::size(),
        InstallSnapshotReqPayload::size(),
        ForwardLeaderMsg::size()
    }
);
constexpr size_t RESP_SIZE = std::max(
    {
        AppendEntriesRespPayload::size(),
        RequestVoteRespPayload::size(),
        InstallSnapshotRespPayload::size()
    }
);

constexpr size_t MAX_INFLIGHT_REQ_BYTES = AppendEntriesReqPayload::size() + sizeof(uint32_t) + sizeof(RpcKind)
    + RequestVoteReqPayload::size() + sizeof(uint32_t) + sizeof(RpcKind)
    + InstallSnapshotReqPayload::size() + sizeof(uint32_t) + sizeof(RpcKind)
    + ForwardLeaderMsg::size() + sizeof(uint32_t) + sizeof(RpcKind);

constexpr size_t MAX_INFLIGHT_RESP_BYTES = AppendEntriesRespPayload::size() + sizeof(uint32_t) + sizeof(RpcKind)
    + RequestVoteRespPayload::size() + sizeof(uint32_t) + sizeof(RpcKind)
    + InstallSnapshotRespPayload::size() + sizeof(uint32_t) + sizeof(RpcKind);

template <SocketType T>
struct ClientConn;

template <>
struct ClientConn<TCP> {
    std::byte rbuf[REQ_SIZE + sizeof(uint32_t) + sizeof(RpcKind)]{};
    std::byte wbuf[MAX_INFLIGHT_RESP_BYTES]{};

    uint64_t client_ip_addr = 0;

    size_t wbuf_offset     =  0; // to track how much of the wbuf has been sent (for chunked sends)
    size_t wbuf_size       =  0; // tracks the number of serialized bytes in wbuf to send over the network
    size_t rbuf_offset     =  0;

    ClientConn* next_free  = nullptr; // freelist link, valid only when free

    FD fd                  = -1;
    uint32_t epoll_events  =  0;
    bool     closing       =  false;

    ~ClientConn() {
        if (fd != -1) ::close(fd);
    }

};

template <>
struct ClientConn<UDP> {
    std::byte wbuf[MAX_INFLIGHT_RESP_BYTES]{};
    uint64_t client_ip_addr = 0;
    size_t wbuf_size        =  0; // tracks the number of serialized bytes in wbuf to send over the network
    ClientConn* next_free   = nullptr; // freelist link, valid only when free
};

/*
pre-allocated, fixed-capacity storage with an intrusive
singly-linked freelist for O(1) acquire/release.

Contract on T:
  - T must be default-constructible.
  - T must expose a public member `T* next_free`. While the slot is on the
    freelist, `next_free` points to the next free slot (or nullptr at the
    tail). While the slot is "in use" the field is unused; convention is
    to leave it as nullptr.

*/
template <typename T>
struct Slab {
    static_assert(std::is_same_v<decltype(std::declval<T&>().next_free), T*>,
                  "Slab<T> requires a public member `T* next_free`");

    explicit Slab(size_t cap) : cap_{cap} {
        buffer_ = new char[sizeof(T) * cap_];
        for (size_t i = 0; i < cap_; ++i) {
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
    Slab(Slab&&)                 = delete;
    Slab& operator=(const Slab&) = delete;
    Slab& operator=(Slab&&)      = delete;

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

    char* buffer_ = nullptr;
    T*    free_head_ = nullptr;
    size_t cap_ = 0;
};

template <SocketType T>
struct ClientConnSlab {
    Slab<ClientConn<T>> slab;

    ClientConnSlab() : slab(MAX_SERVER_CONNS) {}
    ClientConnSlab(ClientConnSlab&&)                 = delete;
    ClientConnSlab& operator=(ClientConnSlab&&)      = delete;
    ClientConnSlab(const ClientConnSlab&)            = delete;
    ClientConnSlab& operator=(const ClientConnSlab&) = delete;

    ClientConn<T>* Acquire() { return slab.Acquire(); }

    void Release(ClientConn<TCP>* c) {
        std::memset(c->rbuf, 0, sizeof(c->rbuf));
        std::memset(c->wbuf, 0, sizeof(c->wbuf));
        c->wbuf_offset = 0;
        c->wbuf_size = 0;
        c->rbuf_offset = 0;
        c->client_ip_addr = 0;
        c->fd = -1;
        c->closing = false;
        c->epoll_events = 0;
        slab.Release(c);
    }

    void Release(ClientConn<UDP>* c) {
        std::memset(c->wbuf, 0, sizeof(c->wbuf));
        c->wbuf_size = 0;
        c->client_ip_addr = 0;
        slab.Release(c);
    }
};

/*
State machine:
  Disconnected -> Connecting -> Connected -> Disconnected (on EOF/error)

  Disconnected: fd == -1. Pending writes queue up in `wbuf`; the loop
                kicks a non-blocking 3) before draining.
  Connecting:   fd >= 0, EPOLLOUT armed. Connection completion arrives
                as EPOLLOUT with SO_ERROR == 0.
  Connected:    EPOLLIN always armed; EPOLLOUT armed iff wbuf_offset <
                wbuf.size().
*/

enum class TimerKind : uint8_t { AE=0, RV=1, IS=2 };

struct TimerFDs {
    FD fds[3] = {-1, -1, -1};

    int get_ae_timeout() {
        return fds[static_cast<uint8_t>(TimerKind::AE)];
    }
    int get_rv_timeout() {
        return fds[static_cast<uint8_t>(TimerKind::RV)];
    }
    int get_is_timeout() {
        return fds[static_cast<uint8_t>(TimerKind::IS)];
    }

    void set_ae_timeout(FD fd) {
        fds[static_cast<uint8_t>(TimerKind::AE)] = fd;
    }
    void set_rv_timeout(FD fd) {
        fds[static_cast<uint8_t>(TimerKind::RV)] = fd;
    }
    void set_is_timeout(FD fd) {
        fds[static_cast<uint8_t>(TimerKind::IS)] = fd;
    }

};

template <SocketType T>
struct PeerConn;

template<>
struct PeerConn<TCP> {
    std::byte wbuf[MAX_INFLIGHT_REQ_BYTES]{};

    std::byte rbuf[RESP_SIZE + sizeof(uint32_t) + sizeof(RpcKind)]{};

    IPAddrPort peer_ip_addr = 0;

    size_t wbuf_offset      = 0;
    size_t wbuf_size        = 0;
    size_t rbuf_offset      = 0;

    NodeID peer_id          = -1;

    FD fd                   = -1;

    TimerFDs timer_fds{};

    uint32_t epoll_events   = 0;

    enum class State : uint8_t { Disconnected, Connecting, Connected };
    State state             = State::Disconnected;

    operator bool() {
        return fd != -1;
    }
    PeerConn(IPAddrPort ip_addr, NodeID peer_id) : peer_ip_addr{ip_addr}, peer_id{peer_id} {}
    PeerConn() = default;
    ~PeerConn() {
        if (fd != -1) ::close(fd);
        for (FD tfd : timer_fds.fds) {
            if (tfd != -1) {
                ::close(tfd);
            }
        }
    }

    PeerConn(const PeerConn& other) = delete;
    PeerConn& operator=(const PeerConn& other) = delete;

    PeerConn(PeerConn&& other) noexcept {
        other.fd = this->fd;
        this->fd = -1;
        for (int i = 0; i < sizeof(timer_fds.fds) / sizeof(FD); ++i) {
            this->timer_fds.fds[i] = other.timer_fds.fds[i];
            other.timer_fds.fds[i] = -1;
        }
    }

    PeerConn& operator=(PeerConn&& other) noexcept {
        if (this != &other) {
            other.fd = this->fd;
            this->fd = -1;
            for (int i = 0; i < sizeof(timer_fds.fds) / sizeof(FD); ++i) {
                this->timer_fds.fds[i] = other.timer_fds.fds[i];
                other.timer_fds.fds[i] = -1;
            }
        }
        return *this;
    }

};

template<>
struct PeerConn<UDP> {
    std::byte wbuf[MAX_INFLIGHT_REQ_BYTES]{};
    std::byte rbuf[RESP_SIZE + sizeof(uint32_t) + sizeof(RpcKind)]{};

    IPAddrPort peer_ip_addr = 0;

    size_t wbuf_size        = 0;

    NodeID peer_id          = -1;

    FD fd                   = -1;

    TimerFDs timer_fds{};

    uint32_t epoll_events   = 0;

    enum class State : uint8_t { Disconnected, Connected };
    State state             = State::Disconnected;

    operator bool() {
        return fd != -1;
    }
    PeerConn(IPAddrPort ip_addr, NodeID peer_id) : peer_ip_addr{ip_addr}, peer_id{peer_id} {}
    PeerConn() = default;
    ~PeerConn() {
        if (fd != -1) ::close(fd);
        for (FD tfd : timer_fds.fds) {
            if (tfd != -1) {
                ::close(tfd);
            }
        }
    }

    PeerConn(const PeerConn& other) = delete;
    PeerConn& operator=(const PeerConn& other) = delete;

    PeerConn(PeerConn&& other) noexcept {
        other.fd = this->fd;
        this->fd = -1;
        for (int i = 0; i < sizeof(timer_fds.fds) / sizeof(FD); ++i) {
            this->timer_fds.fds[i] = other.timer_fds.fds[i];
            other.timer_fds.fds[i] = -1;
        }
    }

    PeerConn& operator=(PeerConn&& other) noexcept {
        if (this != &other) {
            other.fd = this->fd;
            this->fd = -1;
            for (int i = 0; i < sizeof(timer_fds.fds) / sizeof(FD); ++i) {
                this->timer_fds.fds[i] = other.timer_fds.fds[i];
                other.timer_fds.fds[i] = -1;
            }
        }
        return *this;
    }

};
