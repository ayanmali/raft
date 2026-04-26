TODO
- TCP client and server
    - enable TCP_NODELAY, TCP_QUICKACK, TCP_CORK

Done
- connection pooling to limit the # of active sockets at once
    - when creating a socket to reach out to another node, if the pool is full, then prune the least recently used connection
    - LRU cache
    - placement new to reduce allocation syscalls