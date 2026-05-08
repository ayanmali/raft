// #pragma once
// /*
// Binary serialization/deserialization

// Every RPC frame is:
//   uint8_t id          (RPC type tag)
//   fixed-size fields   (network byte order, uint32_t each unless noted)
//   optional length-prefixed trailer (uint32_t length, then `length` opaque bytes)

// Receive side dispatches on `id` and calls a generic visitor with the
// strongly-typed payload.
// */

// #include "./payloads.hpp"
// #include "./conns.hpp"
// #include <arpa/inet.h>
// #include <cerrno>
// #include <cstddef>
// #include <cstdint>
// #include <cstring>
// #include <optional>
// #include <stdexcept>
// #include <sys/socket.h>
// #include <sys/types.h>
// #include <sys/uio.h>
// #include <utility>
// #include <vector>

// static constexpr uint32_t MAX_VECTOR_SIZE_SANITY = 8192;
// static constexpr uint8_t AE_RPC_ID = 1;
// static constexpr uint8_t RV_RPC_ID = 2;
// static constexpr uint8_t IS_RPC_ID = 3;

// namespace detail {

// // Thrown by read_full/write_full when the underlying connection is dead
// // (peer EOF, EPIPE, ECONNRESET, etc). Callers that maintain a connection
// // pool catch this to drop the cached fd and reconnect; everything else
// // still sees a std::runtime_error.
// struct DeadConnError : std::runtime_error {
//     using std::runtime_error::runtime_error;
// };

// // Returns true if `err` (typically errno) indicates the underlying
// // connection is dead and the caller should reconnect rather than retry on
// // the same fd. EAGAIN/EWOULDBLOCK/EINTR are explicitly NOT dead — they
// // mean "try again on the same fd".
// inline bool is_dead_conn(int err) {
//     switch (err) {
//         case EPIPE:
//         case ECONNRESET:
//         case ECONNREFUSED:
//         case ECONNABORTED:
//         case ENOTCONN:
//         case ETIMEDOUT:
//         case EHOSTUNREACH:
//         case ENETUNREACH:
//         case ENETRESET:
//         case EBADF:
//             return true;
//         default:
//             return false;
//     }
// }

// // Drain `iov` of length `iovcnt` from `fd` using readv, looping over partial
// // reads and advancing the iov front as bytes arrive. Throws DeadConnError on
// // peer close or dead-connection errno; std::runtime_error on hard error.
// inline ssize_t read_full(FD fd, iovec* iov, int iovcnt) {
//     ssize_t total = 0;
//     while (iovcnt > 0) {
//         ssize_t n = ::readv(fd, iov, iovcnt);
//         if (n == 0) throw DeadConnError("peer closed connection during read");
//         if (n < 0) {
//             if (errno == EINTR) continue;
//             if (is_dead_conn(errno)) throw DeadConnError("readv: dead connection");
//             throw std::runtime_error("readv failed");
//         }
//         total += n;
//         while (iovcnt > 0 && static_cast<size_t>(n) >= iov->iov_len) {
//             n -= static_cast<ssize_t>(iov->iov_len);
//             ++iov;
//             --iovcnt;
//         }
//         if (iovcnt > 0 && n > 0) {
//             iov->iov_base = static_cast<char*>(iov->iov_base) + n;
//             iov->iov_len  -= static_cast<size_t>(n);
//         }
//     }
//     return total;
// }

// // Mirror of read_full for writev.
// inline ssize_t write_full(FD fd, iovec* iov, int iovcnt) {
//     ssize_t total = 0;
//     while (iovcnt > 0) {
//         ssize_t n = ::writev(fd, iov, iovcnt);
//         if (n < 0) {
//             if (errno == EINTR) continue;
//             if (is_dead_conn(errno)) throw DeadConnError("writev: dead connection");
//             throw std::runtime_error("writev failed");
//         }
//         total += n;
//         while (iovcnt > 0 && static_cast<size_t>(n) >= iov->iov_len) {
//             n -= static_cast<ssize_t>(iov->iov_len);
//             ++iov;
//             --iovcnt;
//         }
//         if (iovcnt > 0 && n > 0) {
//             iov->iov_base = static_cast<char*>(iov->iov_base) + n;
//             iov->iov_len  -= static_cast<size_t>(n);
//         }
//     }
//     return total;
// }

// } // namespace detail

// /*
// AppendEntries wire layout:
//   id (u8) | term | leader_id | prev_log_idx | prev_log_term | leader_commit
//   | entries_len | entries_data...
// */
// inline ssize_t serialize_and_send(const AppendEntriesReqPayload& payload, FD sock_fd) {
//     uint8_t  net_id            = AE_RPC_ID;
//     uint32_t net_term          = htonl(payload.term);
//     uint32_t net_leader_id     = htonl(payload.leader_id);
//     uint32_t net_prev_log_idx  = htonl(payload.prev_log_idx);
//     uint32_t net_prev_log_term = htonl(payload.prev_log_term);
//     uint32_t net_leader_commit = htonl(payload.leader_commit);
//     uint32_t net_entries_len   = htonl(static_cast<uint32_t>(payload.entries.size()));

//     iovec iov[8];
//     iov[0].iov_base = &net_id;            iov[0].iov_len = sizeof(net_id);
//     iov[1].iov_base = &net_term;          iov[1].iov_len = sizeof(net_term);
//     iov[2].iov_base = &net_leader_id;     iov[2].iov_len = sizeof(net_leader_id);
//     iov[3].iov_base = &net_prev_log_idx;  iov[3].iov_len = sizeof(net_prev_log_idx);
//     iov[4].iov_base = &net_prev_log_term; iov[4].iov_len = sizeof(net_prev_log_term);
//     iov[5].iov_base = &net_leader_commit; iov[5].iov_len = sizeof(net_leader_commit);
//     iov[6].iov_base = &net_entries_len;   iov[6].iov_len = sizeof(net_entries_len);
//     iov[7].iov_base = const_cast<std::byte*>(payload.entries.data());
//     iov[7].iov_len  = payload.entries.size();

//     return detail::write_full(sock_fd, iov, 8);
// }

// /*
// RequestVote wire layout:
//   id (u8) | term | candidate_id | last_log_idx | last_log_term
// */
// inline ssize_t serialize_and_send(const RequestVoteReqPayload& payload, FD sock_fd) {
//     uint8_t  net_id            = RV_RPC_ID;
//     uint32_t net_term          = htonl(payload.term);
//     uint32_t net_candidate_id  = htonl(payload.candidate_id);
//     uint32_t net_last_log_idx  = htonl(payload.last_log_idx);
//     uint32_t net_last_log_term = htonl(payload.last_log_term);

//     iovec iov[5];
//     iov[0].iov_base = &net_id;            iov[0].iov_len = sizeof(net_id);
//     iov[1].iov_base = &net_term;          iov[1].iov_len = sizeof(net_term);
//     iov[2].iov_base = &net_candidate_id;  iov[2].iov_len = sizeof(net_candidate_id);
//     iov[3].iov_base = &net_last_log_idx;  iov[3].iov_len = sizeof(net_last_log_idx);
//     iov[4].iov_base = &net_last_log_term; iov[4].iov_len = sizeof(net_last_log_term);

//     return detail::write_full(sock_fd, iov, 5);
// }

// /*
// InstallSnapshot wire layout:
//   id (u8) | term | leader_id | last_included_idx | last_included_term
//   | offset | done (u8) | snapshot_len | snapshot_data...
// */
// inline ssize_t serialize_and_send(const InstallSnapshotReqPayload& payload, FD sock_fd) {
//     uint8_t  net_id                 = IS_RPC_ID;
//     uint32_t net_term               = htonl(payload.term);
//     uint32_t net_leader_id          = htonl(payload.leader_id);
//     uint32_t net_last_included_idx  = htonl(payload.last_included_idx);
//     uint32_t net_last_included_term = htonl(payload.last_included_term);
//     uint32_t net_offset             = htonl(payload.offset);
//     uint8_t  net_done               = payload.done;
//     uint32_t net_snapshot_len       = htonl(static_cast<uint32_t>(payload.snapshot.size()));

//     iovec iov[9];
//     iov[0].iov_base = &net_id;                 iov[0].iov_len = sizeof(net_id);
//     iov[1].iov_base = &net_term;               iov[1].iov_len = sizeof(net_term);
//     iov[2].iov_base = &net_leader_id;          iov[2].iov_len = sizeof(net_leader_id);
//     iov[3].iov_base = &net_last_included_idx;  iov[3].iov_len = sizeof(net_last_included_idx);
//     iov[4].iov_base = &net_last_included_term; iov[4].iov_len = sizeof(net_last_included_term);
//     iov[5].iov_base = &net_offset;             iov[5].iov_len = sizeof(net_offset);
//     iov[6].iov_base = &net_done;               iov[6].iov_len = sizeof(net_done);
//     iov[7].iov_base = &net_snapshot_len;       iov[7].iov_len = sizeof(net_snapshot_len);
//     iov[8].iov_base = const_cast<std::byte*>(payload.snapshot.data());
//     iov[8].iov_len  = payload.snapshot.size();

//     return detail::write_full(sock_fd, iov, 9);
// }

// /*
// Reply wire layouts (no id tag — the client knows which RPC it issued):
//   AppendEntries reply:    term | success (u8)
//   RequestVote reply:      term | vote_granted (u8)
//   InstallSnapshot reply:  term
// */
// inline ssize_t serialize_and_send(const AppendEntriesRespPayload& payload, FD sock_fd) {
//     uint32_t net_term    = htonl(payload.term);
//     uint8_t  net_success = payload.success;

//     iovec iov[2];
//     iov[0].iov_base = &net_term;    iov[0].iov_len = sizeof(net_term);
//     iov[1].iov_base = &net_success; iov[1].iov_len = sizeof(net_success);
//     return detail::write_full(sock_fd, iov, 2);
// }

// inline ssize_t serialize_and_send(const RequestVoteRespPayload& payload, FD sock_fd) {
//     uint32_t net_term         = htonl(payload.term);
//     uint8_t  net_vote_granted = payload.vote_granted;

//     iovec iov[2];
//     iov[0].iov_base = &net_term;         iov[0].iov_len = sizeof(net_term);
//     iov[1].iov_base = &net_vote_granted; iov[1].iov_len = sizeof(net_vote_granted);
//     return detail::write_full(sock_fd, iov, 2);
// }

// inline ssize_t serialize_and_send(const InstallSnapshotRespPayload& payload, FD sock_fd) {
//     uint32_t net_term = htonl(payload.term);

//     iovec iov{ &net_term, sizeof(net_term) };
//     return detail::write_full(sock_fd, &iov, 1);
// }

// // Each deserialize_xx() reads everything *after* the 1-byte RPC id, which
// // the dispatcher has already consumed.

// inline AppendEntriesReqPayload deserialize_ae_req(FD sock_fd) {
//     uint32_t net_term;
//     uint32_t net_leader_id;
//     uint32_t net_prev_log_idx;
//     uint32_t net_prev_log_term;
//     uint32_t net_leader_commit;
//     uint32_t net_entries_len;

//     iovec hdr[6];
//     hdr[0].iov_base = &net_term;          hdr[0].iov_len = sizeof(net_term);
//     hdr[1].iov_base = &net_leader_id;     hdr[1].iov_len = sizeof(net_leader_id);
//     hdr[2].iov_base = &net_prev_log_idx;  hdr[2].iov_len = sizeof(net_prev_log_idx);
//     hdr[3].iov_base = &net_prev_log_term; hdr[3].iov_len = sizeof(net_prev_log_term);
//     hdr[4].iov_base = &net_leader_commit; hdr[4].iov_len = sizeof(net_leader_commit);
//     hdr[5].iov_base = &net_entries_len;   hdr[5].iov_len = sizeof(net_entries_len);
//     detail::read_full(sock_fd, hdr, 6);

//     uint32_t entries_len = ntohl(net_entries_len);
//     if (entries_len > MAX_VECTOR_SIZE_SANITY) {
//         throw std::runtime_error("AppendEntries entries vector exceeds sanity limit; dropping message");
//     }

//     std::vector<std::byte> entries(entries_len);
//     if (entries_len > 0) {
//         iovec trailer{ entries.data(), entries_len };
//         detail::read_full(sock_fd, &trailer, 1);
//     }

//     AppendEntriesReqPayload out{
//         entries,
//         ntohl(net_term),
//         ntohl(net_leader_id),
//         ntohl(net_prev_log_idx),
//         ntohl(net_prev_log_term),
//         ntohl(net_leader_commit),
//     };

//     return out;
// }

// inline RequestVoteReqPayload deserialize_rv_req(FD sock_fd) {
//     uint32_t net_term;
//     uint32_t net_candidate_id;
//     uint32_t net_last_log_idx;
//     uint32_t net_last_log_term;

//     iovec iov[4];
//     iov[0].iov_base = &net_term;          iov[0].iov_len = sizeof(net_term);
//     iov[1].iov_base = &net_candidate_id;  iov[1].iov_len = sizeof(net_candidate_id);
//     iov[2].iov_base = &net_last_log_idx;  iov[2].iov_len = sizeof(net_last_log_idx);
//     iov[3].iov_base = &net_last_log_term; iov[3].iov_len = sizeof(net_last_log_term);
//     detail::read_full(sock_fd, iov, 4);

//     return RequestVoteReqPayload{
//         ntohl(net_term),
//         ntohl(net_candidate_id),
//         ntohl(net_last_log_idx),
//         ntohl(net_last_log_term),
//     };
// }

// inline InstallSnapshotReqPayload deserialize_is_req(FD sock_fd) {
//     uint32_t net_term;
//     uint32_t net_leader_id;
//     uint32_t net_last_included_idx;
//     uint32_t net_last_included_term;
//     uint32_t net_offset;
//     uint8_t  net_done;
//     uint32_t net_snapshot_len;

//     iovec hdr[7];
//     hdr[0].iov_base = &net_term;               hdr[0].iov_len = sizeof(net_term);
//     hdr[1].iov_base = &net_leader_id;          hdr[1].iov_len = sizeof(net_leader_id);
//     hdr[2].iov_base = &net_last_included_idx;  hdr[2].iov_len = sizeof(net_last_included_idx);
//     hdr[3].iov_base = &net_last_included_term; hdr[3].iov_len = sizeof(net_last_included_term);
//     hdr[4].iov_base = &net_offset;             hdr[4].iov_len = sizeof(net_offset);
//     hdr[5].iov_base = &net_done;               hdr[5].iov_len = sizeof(net_done);
//     hdr[6].iov_base = &net_snapshot_len;       hdr[6].iov_len = sizeof(net_snapshot_len);
//     detail::read_full(sock_fd, hdr, 7);

//     uint32_t snapshot_len = ntohl(net_snapshot_len);
//     if (snapshot_len > MAX_VECTOR_SIZE_SANITY) {
//         throw std::runtime_error("InstallSnapshot snapshot vector exceeds sanity limit; dropping message");
//     }

//     std::vector<std::byte> snapshot(snapshot_len);
//     if (snapshot_len > 0) {
//         iovec trailer{ snapshot.data(), snapshot_len };
//         detail::read_full(sock_fd, &trailer, 1);
//     }

//     InstallSnapshotReqPayload out{
//         snapshot,
//         ntohl(net_term),
//         ntohl(net_leader_id),
//         ntohl(net_last_included_idx),
//         ntohl(net_last_included_term),
//         ntohl(net_offset),
//         net_done,
//     };
//     return out;
// }

// // Reply deserializers. No id byte to consume — replies are matched to the
// // in-flight request by the caller, not by an on-wire tag.

// inline AppendEntriesRespPayload deserialize_ae_resp(FD sock_fd) {
//     uint32_t net_term;
//     uint8_t  net_success;

//     iovec iov[2];
//     iov[0].iov_base = &net_term;    iov[0].iov_len = sizeof(net_term);
//     iov[1].iov_base = &net_success; iov[1].iov_len = sizeof(net_success);
//     detail::read_full(sock_fd, iov, 2);

//     return AppendEntriesRespPayload{ ntohl(net_term), net_success };
// }

// inline RequestVoteRespPayload deserialize_rv_resp(FD sock_fd) {
//     uint32_t net_term;
//     uint8_t  net_vote_granted;

//     iovec iov[2];
//     iov[0].iov_base = &net_term;         iov[0].iov_len = sizeof(net_term);
//     iov[1].iov_base = &net_vote_granted; iov[1].iov_len = sizeof(net_vote_granted);
//     detail::read_full(sock_fd, iov, 2);

//     return RequestVoteRespPayload{ ntohl(net_term), net_vote_granted };
// }

// inline InstallSnapshotRespPayload deserialize_is_resp(FD sock_fd) {
//     uint32_t net_term;

//     iovec iov{ &net_term, sizeof(net_term) };
//     detail::read_full(sock_fd, &iov, 1);

//     return InstallSnapshotRespPayload{ ntohl(net_term) };
// }

// template<typename Visitor>
// using Entry = void(*)(Visitor&&);

// template <typename Visitor>
// constexpr auto make_table() {
//     return std::array<Entry<Visitor>, 3>{
//         [](FD sock_fd, Visitor&& v) { v(deserialize_ae_req(sock_fd)); },
//         [](FD sock_fd, Visitor&& v) { v(deserialize_rv_req(sock_fd)); },
//         [](FD sock_fd, Visitor&& v) { v(deserialize_is_req(sock_fd)); },
//     };
// }

// template <typename Visitor>
// void deserialize_req_dispatch(int i, FD sock_fd, Visitor&& v) {
//     static constexpr auto table = make_table<Visitor>();
//     table[i](sock_fd, std::forward<Visitor>(v));
// }

// // Read the 1-byte RPC id, then dispatch to the matching deserialize_xx and pass the result to `v`.
// template <class Visitor>
// inline void deserialize_req_and_receive(FD sock_fd, Visitor&& v) {
//     uint8_t id;
//     iovec iov{ &id, sizeof(id) };
//     detail::read_full(sock_fd, &iov, 1);

//     /* Call example

//     Consuming value (no return): 
//     deserialize_req_dispatch(0, sock_fd, [](auto&& x) {
//     std::cout << x << "\n";
//     });
    
//     */

//     // Calling `v` and passing the corresponding helper's deserialized payload into it
//     switch (id) {
//         case AE_RPC_ID: std::forward<Visitor>(v)(deserialize_ae_req(sock_fd)); break;
//         case RV_RPC_ID: std::forward<Visitor>(v)(deserialize_rv_req(sock_fd)); break;
//         case IS_RPC_ID: std::forward<Visitor>(v)(deserialize_is_req(sock_fd)); break;
//         default: throw std::runtime_error("unknown RPC id");
//     }
// }