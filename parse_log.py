#!/usr/bin/env python3
import os
import struct

LOG_FILE = "/raft/persistence/log"
CMD_SIZE = 4
LOG_ENTRY_SIZE = CMD_SIZE + 4  # data_[CMD_SIZE] + uint32_t term

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

    # Skip the placeholder LogEntry (offset 8)
    entries_offset = 8
    num_entries = (len(raw) - entries_offset) // LOG_ENTRY_SIZE

    print(f"log entries ({num_entries}):")
    for i in range(num_entries):
        off = entries_offset + i * LOG_ENTRY_SIZE
        data_bytes = raw[off:off + CMD_SIZE]
        term = struct.unpack_from("<I", raw, off + CMD_SIZE)[0]
        print(f"  [{i}] data={list(data_bytes)} term={term}")

if __name__ == "__main__":
    main()
