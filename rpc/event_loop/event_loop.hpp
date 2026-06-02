#pragma once

#include "../conns.hpp"
#include "../../cross_thread.hpp"
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>

constexpr int EPOLL_BATCH = 64; // max # of fds processed per loop iteration
constexpr int MAX_ATTEMPTS = 10;

/*
One event loop runs on one thread.
*/
struct EventLoop {
    public:
    EventLoop(FD listen_fd,
              size_t inbound_cap,
              NodeInbox& node_inbox_,
              size_t this_id_);
    ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void Run();
    void Stop(); // event loop can be stopped via a signal on the event fd
    void Wake();
    
    SPSCQueue<std::unique_ptr<RaftMessage>, INBOX_RING_CAP> outbound_inbox{};

    std::unordered_map<NodeID, PeerConn> peer_conns;

    private:
    std::atomic<bool> stopped{false};
    std::atomic<bool> wake_armed{false};

    FD epoll_fd = -1;
    FD listen_fd = -1;
    FD event_fd = -1;
    
    // Inbound
    NodeInbox& node_inbox; // incoming messages

    ClientConnSlab client_slab;
    std::unordered_map<ClientID, ClientConn*> client_conns;
    std::unordered_map<FD, ClientID> client_fd_to_id;
    ClientID next_conn_id = 1;

    size_t this_id;

    // Outbound

    std::unordered_map<FD, NodeID> peer_fd_to_id;
    std::unordered_map<FD, NodeID> peer_timer_fd_to_id; // for heartbeats
    NodeID next_peer_id = 1;

    // ---- helpers ----
    static void set_nonblocking(FD fd);
    void register_fd(FD fd, uint32_t events);
    void modify_client_interest(ClientConn& c, uint32_t events);
    void modify_peer_interest(PeerConn& p, uint32_t events);

    void post_inflight(AppendEntriesReqPayload& payload, NodeID peer_id);
    void post_inflight(RequestVoteReqPayload& payload, NodeID peer_id);
    void post_inflight(InstallSnapshotReqPayload& payload, NodeID peer_id);

    void post_reply(AppendEntriesRespPayload& payload, NodeID client_id);
    void post_reply(RequestVoteRespPayload& payload, NodeID client_id);
    void post_reply(InstallSnapshotRespPayload& payload, NodeID client_id);

    // inbound messaging
    void Accept();
    void OnClientReadable(ClientConn& c);
    void OnClientWritable(ClientConn& c);
    void CloseClient(ClientConn& c);
    void ReapClient(ClientConn& c);

    // outbound messaging
    void OnPeerWritable(PeerConn& p);
    void OnPeerReadable(PeerConn& p);
    void OnPeerTimer(PeerConn& p);
    void StartConnect(PeerConn& p);
    void DropPeer(PeerConn& p);

    // wake / inbox
    void arm_peer_timer(ArmTimerPayload payload, NodeID peer_id);
    void disarm_peer_timer(NodeID peer_id);
    void DrainInbox();
    void OnEventFd();
    void wake_eventfd_unconditional();

    bool post_node_inbox(RpcRequest& req, NodeID client_id);
    bool post_node_inbox(RpcReply& req, NodeID peer_id);
};
