// #include "payloads.hpp"

// template <typename T>
// size_t append_buf(char* out, const T* src, size_t size, size_t p) {

// }

// void serialize(char* buf, AppendEntriesReqPayload& payload) {
//     // TODO: address entries vector sizes that exceed buf size
//     size_t p = 0;
//     p = append_buf(buf, &payload.term, sizeof(payload.term), p);
//     p = append_buf(buf, &payload.leader_id, sizeof(payload.leader_id), p);
//     p = append_buf(buf, &payload.prev_log_idx, sizeof(payload.prev_log_idx), p);
//     p = append_buf(buf, &payload.prev_log_term, sizeof(payload.prev_log_term), p);
//     p = append_buf(buf, &payload.leader_commit, sizeof(payload.leader_commit), p);
//     p = append_buf(buf, &payload.entries.size(), sizeof(payload.entries.size()), p);
//     p = append_buf(buf, payload.entries.data(), sizeof(payload.entries.size()), p);
// }

// void serialize(char* buf, RequestVoteReqPayload& payload) {
//     size_t p = 0;
//     p = append_buf(buf, &payload.term, sizeof(payload.term), p);
//     p = append_buf(buf, &payload.candidate_id, sizeof(payload.candidate_id), p);
//     p = append_buf(buf, &payload.last_log_idx, sizeof(payload.last_log_idx), p);
//     p = append_buf(buf, &payload.last_log_term, sizeof(payload.last_log_term), p);

// }

// void serialize(char* buf, InstallSnapshotReqPayload& payload) {
//     // TODO: address entries vector sizes that exceed buf size
//     size_t p = 0;
//     p = append_buf(buf, &payload.term, sizeof(payload.term), p);
//     p = append_buf(buf, &payload.leader_id, sizeof(payload.leader_id), p);
//     p = append_buf(buf, &payload.last_included_idx, sizeof(payload.last_included_idx), p);
//     p = append_buf(buf, &payload.last_included_term, sizeof(payload.last_included_term), p);
//     p = append_buf(buf, &payload.offset, sizeof(payload.offset), p);
//     p = append_buf(buf, &payload.done, sizeof(payload.done), p);
// }