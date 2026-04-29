TODO

- serialization/deserialization across the network; scatter/gather with writev
- TCP client and server
  - enable TCP_QUICKACK, TCP_CORK
- Replace std vectors with stack-allocated arrays or memory pools

Done

- connection pooling to limit the # of active client sockets at once
  - when creating a socket to reach out to another node, if the pool is full, then prune the least recently used connection
  - LRU cache
  - placement new to reduce allocation syscalls
- Producing and consuming requests into/out of the thread pool queues uses an inline stack-allocated buffer (size INLINE_PAYLOAD_BYTES), and if it overflows, falls back to a heap-allocated buffer.
- **ConnSlab** in `rpc/conns.hpp` pre-allocates `MAX_CONNECTIONS` `Connection` objects with placement new, threads a freelist through `next_free`, and reuses them across accepts. The `conns` map now holds raw pointers; the slab owns lifetime:

