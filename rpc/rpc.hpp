#pragma once
/*
Client-side `send_*_rpc` entry points and the small Node-level setup helpers.

Each `send_*_rpc` resolves a peer fd (via the LRU connection pool, with
connect-on-miss), writev's the request via `serialize_and_send` from
protocol.hpp, then readv's the matching reply via `deserialize_*_resp`
and returns it.

Wire formats and (de)serialization helpers live in protocol.hpp.
Payload struct definitions live in payloads.hpp.
*/
#include "../node.hpp"
#include "./client.hpp"
#include "./payloads.hpp"
#include "./protocol.hpp"
#include "client/client.hpp"
#include <initializer_list>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include <unistd.h>

inline void Node::setup_sockets() {
    // Only the listening socket is created here. Per-peer client sockets are
    // created lazily by `peer_fd()` and cached in `client_conns`.
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    handle_setup_errs({server_fd});
}

inline void Node::handle_setup_errs(std::initializer_list<int> sock_fds) {
    for (auto fd : sock_fds) {
        if (fd < 0) throw std::runtime_error("Error setting up socket fds");
    }
}

inline int Node::peer_fd(std::string_view peer_ip) {
    int fd = client_conns.get(peer_ip);
    if (fd < 0) {
        fd = detail::connect_new(peer_ip, SERVER_PORT);
        int evicted = -1;
        client_conns.set(peer_ip, fd, &evicted);
        if (evicted >= 0) ::close(evicted);
    }
    return fd;
}

inline AppendEntriesRespPayload Node::send_append_entries_rpc(std::string_view peer_ip) {
    const AppendEntriesReqPayload payload = AppendEntriesReqPayload();
    int fd = peer_fd(peer_ip);
    serialize_and_send(payload, fd);
    return deserialize_ae_resp(fd);
}

inline RequestVoteRespPayload Node::send_request_vote_rpc(std::string_view peer_ip) {
    const RequestVoteReqPayload payload = RequestVoteReqPayload();
    int fd = peer_fd(peer_ip);
    serialize_and_send(payload, fd);
    return deserialize_rv_resp(fd);
}

inline InstallSnapshotRespPayload Node::send_install_snapshot_rpc(std::string_view peer_ip) {
    const InstallSnapshotReqPayload payload = InstallSnapshotReqPayload();
    int fd = peer_fd(peer_ip);
    serialize_and_send(payload, fd);
    return deserialize_is_resp(fd);
}
