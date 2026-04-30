TODO

- serialization/deserialization across the network
  - reply-side logic
- TCP client and server
  - enable TCP_QUICKACK, TCP_CORK
- Replace std vectors with stack-allocated arrays or memory pools
- separate sockets for IS RPCs 

Done

- TCP client and server
  - enable tcp no delay
- connection pooling to limit the # of active client sockets at once
  - when creating a socket to reach out to another node, if the pool is full, then prune the least recently used connection
  - LRU cache
  - placement new to reduce allocation syscalls
- Producing and consuming requests into/out of the thread pool queues uses an inline stack-allocated buffer (size INLINE_PAYLOAD_BYTES), and if it overflows, falls back to a heap-allocated buffer.
- **`Slab<T>`** in `rpc/slab.hpp` is the shared slot allocator: pre-allocated buffer, intrusive `next_free` freelist, O(1) Acquire/Release. Both sides of the RPC layer build on it:
  - **`ConnSlab`** in `rpc/server/conns.hpp` is a thin wrapper over `Slab<Connection>` that adds per-Connection reset on Release. Pre-allocates `MAX_SERVER_CONNS` slots; the `conns` map holds raw pointers and the slab owns lifetime.
  - **`ConnectionPool`** in `rpc/client/conn_pool.hpp` uses `Slab<LRUNode>` for storage and layers a doubly-linked LRU list plus a hash-map index on top, so the bounded peer cache keeps the same eviction policy without re-implementing the freelist.

