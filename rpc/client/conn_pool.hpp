#pragma once
#include <cstddef>
#include <string_view>
#include <unordered_map>
#include <unistd.h>

#include "../slab.hpp"

/*
Fixed-capacity LRU mapping from peer IP (std::string_view) to a cached
client-side socket fd (int).

Keys are non-owning std::string_view. The caller is responsible for keeping
the underlying string storage alive for the lifetime of the entry. The
static peer list in config.hpp satisfies this. If peers ever become
dynamic, switch the key type to std::string.

Storage: nodes are slab-allocated via Slab<LRUNode> (see rpc/slab.hpp).
The slab handles raw allocation and the intrusive `next_free` freelist;
this struct layers an LRU doubly-linked list (head=MRU, tail=LRU) and a
hash-map index on top.

Operations:
  - get(ip)        : O(1) map lookup; on hit, promote to head and return fd.
  - set(ip, fd)    : O(1). If key exists, update + promote. If new and
                     the slab has a free slot, acquire it. If new and the
                     pool is full, evict tail in place and reuse it.
  - remove(ip)     : O(1). Unlink from LRU list, release slot to the slab,
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
    Slab<LRUNode> slab;
    LRUNode*      head      = nullptr;        // MRU
    LRUNode*      tail      = nullptr;        // LRU
    int           size      = 0;              // # of entries in the LRU list
    int           capacity  = 0;
    std::unordered_map<std::string_view, LRUNode*> map;

    explicit ConnectionPool(int cap);

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
};

inline ConnectionPool::ConnectionPool(int cap)
    : slab(static_cast<size_t>(cap)), capacity{cap} {
    map.reserve(static_cast<size_t>(capacity));
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

    if (LRUNode* n = slab.Acquire()) {
        n->key   = ip_addr;
        n->value = fd;
        push_front(n);
        map.emplace(ip_addr, n);
        ++size;
        return;
    }

    // Pool full: evict LRU tail and reuse its slot in place. The slot
    // never returns to the slab's freelist here — it stays "in use" with
    // a new key/fd.
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
    n->key   = {};
    n->value = -1;
    slab.Release(n);
    --size;
    return fd;
}

inline void ConnectionPool::close_all() {
    // Slots not in `map` are already on the slab's freelist; only the
    // in-use ones need to be closed and released.
    for (auto& [k, n] : map) {
        if (n->value >= 0) ::close(n->value);
        n->key   = {};
        n->value = -1;
        unlink(n);
        slab.Release(n);
    }
    map.clear();
    size = 0;
    // head/tail are nullptr after the unlinks above.
}
