#!/usr/bin/env python3
"""
Parse and print the contents of a Raft snapshot file.

Format (from core/main_loop.cpp log-compaction path):
  1. cluster_bitmap_size: size_t (8 bytes LE) -- byte count of the bitmap
  2. cluster_bitmap:      cluster_bitmap_size bytes (node_ids_ bitset)
  3. last_applied_idx:    uint32 LE
  4. last_applied_term:   uint32 LE
  5. state_machine_data:  variable-length (written by create_snapshot callback)
"""
import struct
import sys

SNAPSHOT_PATH = "/root/raft/persistence/snapshot"

def parse_snapshot(path: str):
    with open(path, "rb") as f:
        data = f.read()

    offset = 0

    # 1. cluster bitmap size (size_t, typically 8 bytes on 64-bit)
    if len(data) - offset < struct.calcsize("P"):
        print(f"Snapshot file too small ({len(data)} bytes) to contain header")
        return
    cluster_size_bytes = struct.unpack_from("P", data, offset)[0]
    offset += struct.calcsize("P")

    # 2. cluster bitmap
    cluster_bitmap = data[offset:offset + cluster_size_bytes]
    offset += cluster_size_bytes

    # Decode the bitset: each bit represents a node ID
    node_ids = []
    for byte_idx in range(len(cluster_bitmap)):
        byte = cluster_bitmap[byte_idx]
        for bit in range(8):
            if byte & (1 << bit):
                node_ids.append(byte_idx * 8 + bit)

    # 3. last_applied_idx (uint32 LE)
    last_applied_idx = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # 4. last_applied_term (uint32 LE)
    last_applied_term = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # 5. state machine data (everything remaining)
    state_data = data[offset:]

    print("=== Snapshot File ===")
    print(f"File size:              {len(data)} bytes")
    print(f"Bitmap size (size_t):   {cluster_size_bytes}")
    print(f"Header bytes consumed:  {offset}")
    print(f"State data:             {len(state_data)} bytes")
    print()
    print(f"Cluster bitmap ({cluster_size_bytes} bytes):")
    print(f"  raw bytes:    {cluster_bitmap.hex(' ')}")
    print(f"  node IDs set: {node_ids}")
    print()
    print(f"last_applied_idx:  {last_applied_idx}")
    print(f"last_applied_term: {last_applied_term}")
    print()
    print("State machine data:")
    if len(state_data) <= 64:
        print(f"  raw bytes:    {state_data.hex(' ')}")
    else:
        print(f"  raw bytes:    {state_data[:64].hex(' ')} ... ({len(state_data)} total)")
    print(f"  as hex:       {' '.join(f'{b:02x}' for b in state_data)}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else SNAPSHOT_PATH
    parse_snapshot(path)
