#!/usr/bin/env python3
"""
Parse and print the contents of a Raft snapshot file.

Format (from core/main_loop.cpp log-compaction path):
  1. cluster_bitmap:  total_size bytes (node_ids_ bitset)
  2. last_applied_idx: uint32 LE
  3. last_applied_term: uint32 LE
  4. state_machine_data: variable-length (written by create_snapshot callback)

total_size = ((BASE_CLUSTER_SIZE - 1) / 8) + 1) * 8
With BASE_CLUSTER_SIZE=3, total_size = 8 bytes.
"""
import struct
import sys

SNAPSHOT_PATH = "/root/raft/persistence/snapshot"

def parse_snapshot(path: str, cluster_bitmap_bytes: int = 8):
    with open(path, "rb") as f:
        data = f.read()

    offset = 0

    # 1. cluster bitmap
    bitmap_bytes = cluster_bitmap_bytes
    cluster_bitmap = data[offset:offset + bitmap_bytes]
    offset += bitmap_bytes

    # Decode the bitset: each bit represents a node ID
    node_ids = []
    for byte_idx in range(len(cluster_bitmap)):
        byte = cluster_bitmap[byte_idx]
        for bit in range(8):
            if byte & (1 << bit):
                node_ids.append(byte_idx * 8 + bit)
    # node_ids_ also has a member num_set == len(node_ids)

    # 2. last_applied_idx (uint32 LE)
    last_applied_idx = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # 3. last_applied_term (uint32 LE)
    last_applied_term = struct.unpack_from("<I", data, offset)[0]
    offset += 4

    # 4. state machine data (everything remaining)
    state_data = data[offset:]

    print("=== Snapshot File ===")
    print(f"File size:      {len(data)} bytes")
    print(f"Header size:    {offset} bytes")
    print(f"State data:     {len(state_data)} bytes")
    print()
    print("Cluster bitmap ({:3d} bytes):".format(len(cluster_bitmap)))
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
    bitmap_bytes = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    parse_snapshot(path, bitmap_bytes)
