#pragma once
// using NodeRequestInbox = MPSC<RpcReply
#include "queues/mpsc.hpp"
#include "rpc/protocol/utils.hpp"

constexpr size_t RECV_CHUNK = 4096;
constexpr size_t INBOX_RING_CAP = 64; // per producer; must be power of 2

template <uint N>
using NodeReplyInbox = MPSC<RpcReply, INBOX_RING_CAP, N>;

struct ReplyHandlerVisitor;