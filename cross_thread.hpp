#pragma once
// using NodeRequestInbox = MPSC<RpcReply
#include "queues/mpsc.hpp"
#include "rpc/protocol/utils.hpp"

constexpr size_t RECV_CHUNK = 4096;
constexpr size_t INBOX_RING_CAP = 64; // per producer; must be power of 2

// for processing incoming requests/replies
using NodeInbox = MPSC<std::unique_ptr<RpcMessage>, INBOX_RING_CAP, EVENT_LOOP_THREADS>;

struct ReplyHandlerVisitor;
struct RequestHandlerVisitor;