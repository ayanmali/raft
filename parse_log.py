#!/usr/bin/env python3
import os
import struct

LOG_FILE = "/raft/persistence/log"
SNAPSHOT_FILE = "/raft/persistence/snapshot"
CMD_SIZE = 4
LOG_ENTRY_SIZE = CMD_SIZE + 4  # data_[CMD_SIZE] + uint32_t term

def read_last_applied_idx():
    """Read last_applied_idx (uint32 LE) from the snapshot file.

    Returns 0 if the snapshot is missing or too small to hold the header,
    matching the node's default last_applied_idx_ = 0.
    """
    if not os.path.exists(SNAPSHOT_FILE):
        return 0
    with open(SNAPSHOT_FILE, "rb") as f:
        raw = f.read(4)
    if len(raw) < 4:
        return 0
    return struct.unpack_from("<I", raw, 0)[0]

def main():
    if not os.path.exists(LOG_FILE):
        print(f"File {LOG_FILE} not found")
        return

    with open(LOG_FILE, "rb") as f:
        raw = f.read()

    size_bytes = os.path.getsize(LOG_FILE)
    print(f"Log file size: {size_bytes} bytes")

    # current_term_: uint32_t
    current_term = struct.unpack_from("<I", raw, 0)[0]

    # voted_for_: int32_t
    voted_for = struct.unpack_from("<i", raw, 4)[0]

    print(f"current_term: {current_term}")
    print(f"voted_for:    {voted_for}")
    print()

    # Logical indexes are 1-based; entries physically stored in the log
    # start at base_logical_idx_ = last_applied_idx_ + 1 (the snapshot
    # covers everything up to and including last_applied_idx_).
    last_applied_idx = read_last_applied_idx()
    base_logical_idx = last_applied_idx + 1
    print(f"snapshot last_applied_idx: {last_applied_idx}")
    print(f"base_logical_idx:          {base_logical_idx}")
    print()

    # Skip the 8-byte header (current_term_ + voted_for_)
    entries_offset = 8
    num_entries = (len(raw) - entries_offset) // LOG_ENTRY_SIZE

    print(f"log entries ({num_entries}):")
    for i in range(num_entries):
        off = entries_offset + i * LOG_ENTRY_SIZE
        data_bytes = raw[off:off + CMD_SIZE]
        term = struct.unpack_from("<I", raw, off + CMD_SIZE)[0]
        logical_idx = base_logical_idx + i
        print(f"  [{logical_idx}] data={list(data_bytes)} term={term}")

if __name__ == "__main__":
    main()
