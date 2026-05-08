#pragma once
/*
Stale: superseded by the per-thread EventLoop architecture.

The earlier design here had Node hold a single listening socket, a single
EventLoop, and bind_and_listen()/server_expose() helpers that wired one
socket through one epoll loop. That's been replaced by:

  - node.hpp           : Node<N> creates N listening sockets via
    SO_REUSEPORT inside setup_listen_sockets() and constructs an
    EventLoop per worker thread.
  - rpc/event_loop.hpp : self-contained EventLoop that drives one
    listen fd, one eventfd, accepted client fds, and outbound peer
    fds for its peer subset (peer_id % N).

This file is kept as a no-op marker so that any lingering include
references compile cleanly while the migration settles. New code
should not include it.
*/
