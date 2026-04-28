TODO
- TCP client and server
    - enable TCP_NODELAY, TCP_QUICKACK, TCP_CORK
- Task data is a heap-allocated payload currently allocated with `new[]`. Refactor this to use a slab allocator instead, and/or see if there's a way to have it stack-allocated or reuse the same memory to avoid repetitive new/delete calls.

- Replace std vectors with stack-allocated arrays or memory pools

Done
- connection pooling to limit the # of active sockets at once
    - when creating a socket to reach out to another node, if the pool is full, then prune the least recently used connection
    - LRU cache
    - placement new to reduce allocation syscalls