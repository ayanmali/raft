#pragma once
#include <cstddef>
#include <string_view>
#include <unordered_map>
#include <unistd.h>
#include "../config.hpp"

/*
Fixed-capacity LRU mapping from peer IP (std::string_view) to a cached
client-side socket fd (int).

Keys are non-owning std::string_view. The caller is responsible for keeping
the underlying string storage alive for the lifetime of the entry. The
static peer list in config.hpp satisfies this. If peers ever become
dynamic, switch the key type to std::string.

Memory: nodes are placement-new'd into one pre-allocated buffer in the
ctor; nothing allocates or frees per-operation. Two pointer chains are
maintained:
  - The doubly-linked LRU list (head=MRU, tail=LRU). Used only for ordering
    and tail-eviction.
  - An intrusive singly-linked freelist (next_free), used for O(1)
    acquire/release of vacant slots. Mirrors the ConnSlab pattern in
    rpc/conns.hpp.

Operations:
  - get(ip)        : O(1) map lookup; on hit, promote to head and return fd.
  - set(ip, fd)    : O(1). If key exists, update + promote. If new and
                     freelist is non-empty, pop a slot. If new and the pool
                     is full, evict tail and reuse it.
  - remove(ip)     : O(1). Unlink from LRU list, push slot onto freelist,
                     and erase from map. Caller closes the fd.
*/

struct LRUNode {
    std::string_view key{};
    int              value     = -1;
    LRUNode*         prev      = nullptr;   // LRU order
    LRUNode*         next      = nullptr;   // LRU order
    LRUNode*         next_free = nullptr;   // freelist (only valid when free)
};

struct ConnectionPool {
    char*       buffer    = nullptr;
    LRUNode*    head      = nullptr;        // MRU
    LRUNode*    tail      = nullptr;        // LRU
    LRUNode*    free_head = nullptr;        // top of freelist
    int         size      = 0;              // # of entries in the LRU list
    int         capacity  = 0;
    std::unordered_map<std::string_view, LRUNode*> map;

    explicit ConnectionPool(int cap);
    ~ConnectionPool();

    ConnectionPool(const ConnectionPool&)            = delete;
    ConnectionPool& operator=(const ConnectionPool&) = delete;

    // Returns the cached fd for ip_addr, or -1 on miss. On hit the entry is
    // promoted to MRU.
    int  get(std::string_view ip_addr);

    // Inserts or updates the cached fd for ip_addr and promotes it to MRU.
    // If the pool is at capacity for a new key, the LRU tail entry is
    // evicted and the caller is given back the evicted fd via *evicted_fd
    // (so the caller can close() it). Pass nullptr to ignore.
    void set(std::string_view ip_addr, int fd, int* evicted_fd = nullptr);

    // Drops the entry for ip_addr from the pool. Returns the fd that was
    // cached (caller must close), or -1 if there was no entry.
    int  remove(std::string_view ip_addr);

    // Closes every cached fd and clears the pool. Used at shutdown.
    void close_all();

private:
    void unlink(LRUNode* n) {
        if (n->prev) n->prev->next = n->next; else head = n->next;
        if (n->next) n->next->prev = n->prev; else tail = n->prev;
        n->prev = n->next = nullptr;
    }
    void push_front(LRUNode* n) {
        n->prev = nullptr;
        n->next = head;
        if (head) head->prev = n;
        else      tail = n;
        head = n;
    }
    LRUNode* acquire_free() {
        if (!free_head) return nullptr;
        LRUNode* n = free_head;
        free_head = n->next_free;
        n->next_free = nullptr;
        return n;
    }
    void release_free(LRUNode* n) {
        n->key       = {};
        n->value     = -1;
        n->next_free = free_head;
        free_head    = n;
    }
};

inline ConnectionPool::ConnectionPool(int cap) : capacity{cap} {
    map.reserve(static_cast<size_t>(capacity));

    buffer = new char[sizeof(LRUNode) * static_cast<size_t>(capacity)];
    for (int i = 0; i < capacity; ++i) {
        auto* n = ::new (buffer + i * sizeof(LRUNode)) LRUNode();
        n->next_free = free_head;
        free_head = n;
    }
}

inline ConnectionPool::~ConnectionPool() {
    // Walk by buffer index — pointer chains may be split between the LRU
    // list and the freelist by this point.
    for (int i = 0; i < capacity; ++i) {
        reinterpret_cast<LRUNode*>(buffer + i * sizeof(LRUNode))->~LRUNode();
    }
    delete[] buffer;
    buffer = nullptr;
}

inline int ConnectionPool::get(std::string_view ip_addr) {
    auto it = map.find(ip_addr);
    if (it == map.end()) return -1;
    LRUNode* n = it->second;
    unlink(n);
    push_front(n);
    return n->value;
}

inline void ConnectionPool::set(std::string_view ip_addr, int fd, int* evicted_fd) {
    if (evicted_fd) *evicted_fd = -1;

    if (auto it = map.find(ip_addr); it != map.end()) {
        LRUNode* n = it->second;
        n->value = fd;
        unlink(n);
        push_front(n);
        return;
    }

    if (LRUNode* n = acquire_free()) {
        n->key   = ip_addr;
        n->value = fd;
        push_front(n);
        map.emplace(ip_addr, n);
        ++size;
        return;
    }

    // Pool full: evict LRU tail and reuse its slot for the new key.
    LRUNode* n = tail;
    if (evicted_fd) *evicted_fd = n->value;
    map.erase(n->key);
    unlink(n);
    n->key   = ip_addr;
    n->value = fd;
    push_front(n);
    map.emplace(ip_addr, n);
}

inline int ConnectionPool::remove(std::string_view ip_addr) {
    auto it = map.find(ip_addr);
    if (it == map.end()) return -1;
    LRUNode* n = it->second;
    int fd = n->value;
    map.erase(it);
    unlink(n);
    release_free(n);
    --size;
    return fd;
}

inline void ConnectionPool::close_all() {
    for (auto& [k, n] : map) {
        if (n->value >= 0) ::close(n->value);
    }
    map.clear();
    // Reset both lists to "all free".
    head = tail = nullptr;
    free_head = nullptr;
    for (int i = 0; i < capacity; ++i) {
        auto* n = reinterpret_cast<LRUNode*>(buffer + i * sizeof(LRUNode));
        n->key       = {};
        n->value     = -1;
        n->prev      = nullptr;
        n->next      = nullptr;
        n->next_free = free_head;
        free_head    = n;
    }
    size = 0;
}
