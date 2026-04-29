/* 
Binary serialization/deserialization

Every RPC comes with a header and payload.
Header includes:
-  an ID to indicate what kind
of RPC the network payload is

deserialization is unique for each RPC type.
*/

#include "rpc.hpp"
#include <cstddef>
#include <cstdint>
#include <sys/types.h>

static constexpr uint8_t AE_RPC_ID = 1;
static constexpr uint8_t RV_RPC_ID = 2;
static constexpr uint8_t IS_RPC_ID = 3;

static constexpr uint32_t AE_WRITEV_BUFS = 8;
static constexpr uint32_t RV_WRITEV_BUFS = 5;
static constexpr uint32_t IS_WRITEV_BUFS = 9;

/*
Raw form:
RPC ID
Entries vector length
Entries vector data
Term #
Leader ID
Previous log index
Previous log term
Leader commit index
*/
inline ssize_t AppendEntriesPayload::serialize_and_send(int sock_fd) {
    // Convert fields into safe format
    auto net_id = htonl(AE_RPC_ID);
    auto net_entries_len = htonl(entries.size());
    auto net_term = htonl(term);
    auto net_leader_id = htonl(leader_id);
    auto net_prev_log_idx = htonl(prev_log_idx);
    auto net_prev_log_term = htonl(prev_log_term);
    auto net_leader_commit = htonl(leader_commit);

    // Sending on the socket
    iovec iov[AE_WRITEV_BUFS];

    // Header
    iov[0].iov_base = &net_id;
    iov[0].iov_len = sizeof(net_id);

    // Payload data

    // vector stored as length, followed by data
    iov[1].iov_base = &net_entries_len;
    iov[1].iov_len = sizeof(net_entries_len);

    iov[2].iov_base = entries.data();
    iov[2].iov_len = net_entries_len;

    iov[3].iov_base = &net_term;
    iov[3].iov_len = sizeof(net_term);

    iov[4].iov_base = &net_leader_id;
    iov[4].iov_len = sizeof(net_leader_id);

    iov[5].iov_base = &net_prev_log_idx;
    iov[5].iov_len = sizeof(net_prev_log_idx);

    iov[6].iov_base = &net_prev_log_term;
    iov[6].iov_len = sizeof(net_prev_log_term);

    iov[7].iov_base = &net_leader_commit;
    iov[7].iov_len = sizeof(net_leader_commit);

    auto bytes_sent = writev(sock_fd, iov, AE_WRITEV_BUFS);
    return bytes_sent;

}

inline ssize_t RequestVotePayload::serialize_and_send(int sock_fd) {
    // Convert fields into safe format
    auto net_id = htonl(RV_RPC_ID);
    auto net_term = htonl(term);
    auto net_candidate_id = htonl(candidate_id);
    auto net_last_log_idx = htonl(last_log_idx);
    auto net_last_log_term = htonl(last_log_term);

    // Sending on the socket
    iovec iov[RV_WRITEV_BUFS];

    // Header
    iov[0].iov_base = &net_id;
    iov[0].iov_len = sizeof(net_id);

    // Payload data
    iov[1].iov_base = &net_term;
    iov[1].iov_len = sizeof(net_term);

    iov[2].iov_base = &net_candidate_id;
    iov[2].iov_len = sizeof(net_candidate_id);

    iov[3].iov_base = &net_last_log_idx;
    iov[3].iov_len = sizeof(net_last_log_idx);

    iov[4].iov_base = &net_last_log_term;
    iov[4].iov_len = sizeof(net_last_log_term);

    auto bytes_sent = writev(sock_fd, iov, RV_WRITEV_BUFS);
    return bytes_sent;

}

inline ssize_t InstallSnapshotPayload::serialize_and_send(int sock_fd) {
    // Convert fields into safe format
    auto net_id = htonl(AE_RPC_ID);
    auto net_snapshot_len = htonl(snapshot.size());
    auto net_term = htonl(term);
    auto net_leader_id = htonl(leader_id);
    auto net_last_included_idx = htonl(last_included_idx);
    auto net_last_included_term = htonl(last_included_term);
    auto net_offset = htonl(offset);
    auto net_done = htonl(done);

    // Sending on the socket
    iovec iov[IS_WRITEV_BUFS];

    // Header
    iov[0].iov_base = &net_id;
    iov[0].iov_len = sizeof(net_id);

    // Payload data

    // vector stored as length, followed by data
    iov[1].iov_base = &net_snapshot_len;
    iov[1].iov_len = sizeof(net_snapshot_len);

    iov[2].iov_base = snapshot.data();
    iov[2].iov_len = net_snapshot_len;

    iov[3].iov_base = &net_term;
    iov[3].iov_len = sizeof(net_term);

    iov[4].iov_base = &net_leader_id;
    iov[4].iov_len = sizeof(net_leader_id);

    iov[5].iov_base = &net_last_included_idx;
    iov[5].iov_len = sizeof(net_last_included_idx);

    iov[6].iov_base = &net_last_included_term;
    iov[6].iov_len = sizeof(net_last_included_term);

    iov[7].iov_base = &net_offset;
    iov[7].iov_len = sizeof(net_offset);

    iov[8].iov_base = &net_done;
    iov[8].iov_len = sizeof(net_done);

    auto bytes_sent = writev(sock_fd, iov, IS_WRITEV_BUFS);
    return bytes_sent;

}