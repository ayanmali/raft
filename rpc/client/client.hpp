#pragma once
/*
Outbound client-side RPC dispatch.

Historically this file held blocking `Node::send_*_rpc` implementations
that resolved a peer fd via an LRU cache and called the readv/writev
helpers in protocol.hpp. That blocking model is incompatible with the
per-thread epoll loop architecture: a slow peer would stall the entire
loop, preventing it from servicing inbound clients or other peers.

The new model lives in:

  - rpc/event_loop.hpp : EventLoop::EnqueueAE/EnqueueRV/EnqueueIS push a
    serialized request + reply callback into the owning loop's inbox
    (peer_id % N -> loop index) and wake the loop via eventfd.
  - node.hpp           : Node<N>::send_*_rpc are thin shims that resolve
    the owning loop and call the matching Enqueue* method.

The reply callback fires on the owning loop's thread once the response
bytes are fully received and parsed. Callbacks acquire `Node::state_mu_`
to update Raft state; they must not block on any other event-loop
thread (which would deadlock if both are waiting on each other's
inboxes).
*/
