#pragma once

#include "../conns.hpp"
#include "../../cross_thread.hpp"
#include "../../errors.hpp"
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
#include <sys/epoll.h>
#include <sys/eventfd.h>

constexpr int EPOLL_BATCH = 64; // max # of fds processed per loop iteration
constexpr int MAX_ATTEMPTS = 10;

/*
One event loop runs on one thread.
*/
struct EventLoop {
    public:
    static std::expected<std::unique_ptr<EventLoop>, std::string> CreateEventLoop(size_t inbound_cap, NodeInbox& node_inbox, size_t this_id_, long heartbeat_period_);
    ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    VoidExpectedF Run();
    void Stop(); // event loop can be stopped via a signal on the event fd
    void Wake();
    VoidExpectedF AddPeer(NodeID id, const char* ip_addr, const char* port);

    SPSCQueue<std::unique_ptr<RaftMessage>, INBOX_RING_CAP> outbound_inbox{};

    std::atomic<bool> stopped{false};

    private:
    ClientConnSlab client_slab;
    std::unordered_map<NodeID, PeerConn> peer_conns;

    std::unordered_map<ClientID, ClientConn*> client_conns;
    std::unordered_map<FD, ClientID> client_fd_to_id;
    std::atomic<bool> wake_armed{false};

    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd = -1;

    // Inbound
    NodeInbox& node_inbox; // incoming messages

    ClientID next_conn_id = 0;

    const size_t this_id;

    // Outbound

    std::unordered_map<FD, NodeID> peer_fd_to_id;
    std::unordered_map<FD, NodeID> peer_timer_fd_to_id; // for heartbeats

    const long heartbeat_period;

    EventLoop(size_t inbound_cap, NodeInbox&, size_t this_id, long period);
    // ---- helpers ----
    static VoidExpected set_nonblocking(FD fd);
    VoidExpected register_fd(FD fd, uint32_t events);
    VoidExpected modify_client_interest(ClientConn* c, uint32_t events);
    VoidExpected modify_peer_interest(PeerConn& p, uint32_t events);

    VoidExpectedF post_inflight(AppendEntriesReqPayload& payload, NodeID peer_id);
    VoidExpectedF post_inflight(RequestVoteReqPayload& payload, NodeID peer_id);
    VoidExpectedF post_inflight(InstallSnapshotReqPayload& payload, NodeID peer_id);

    VoidExpectedF post_reply(AppendEntriesRespPayload& payload, NodeID client_id);
    VoidExpectedF post_reply(RequestVoteRespPayload& payload, NodeID client_id);
    VoidExpectedF post_reply(InstallSnapshotRespPayload& payload, NodeID client_id);

    // inbound messaging
    VoidExpected setup_listen_socket();
    VoidExpected Accept();
    VoidExpected OnClientReadable(ClientConn* c);
    VoidExpected OnClientWritable(ClientConn* c);
    void CloseClient(ClientConn* c);
    void ReapClient(ClientConn* c);

    // outbound messaging
    VoidExpected OnPeerWritable(PeerConn& p);
    VoidExpected OnPeerReadable(PeerConn& p);
    VoidExpected OnPeerTimer(PeerConn& p);
    VoidExpectedF StartConnect(PeerConn& p);
    void DropPeer(PeerConn& p);

    // wake / inbox
    VoidExpectedF arm_heartbeat_timer(NodeID peer_id);
    VoidExpectedF disarm_heartbeat_timer(NodeID peer_id);
    VoidExpectedF DrainInbox();
    VoidExpectedF OnEventFd();
    void wake_eventfd_unconditional();

    // Accepts any RpcRequest/RpcReply variant, or a bare payload (e.g.
    // DropPeerMsg), and widens it into the RpcMessage variant before pushing.
    template <typename T>
    bool post_node_inbox(T&& msg, NodeID node_id) {
        RpcMessage widened = to_rpc_message(std::forward<T>(msg));
        for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
            if (node_inbox.Push(this_id,
                    std::make_unique<RaftMessage>(std::move(widened), node_id))) {
                return true;
            }
        }
        return false;
    }
};
