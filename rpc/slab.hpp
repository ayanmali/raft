#pragma once
#include <cstddef>
#include <new>
#include <type_traits>

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
