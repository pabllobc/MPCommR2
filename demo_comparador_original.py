"""Demonstrate the original comparisonServer coverage defects with synthetic logs."""

from __future__ import annotations

import ast
import pathlib


def original_compare(msgs, n_peers, n_msgs):
    unordered = 0
    for j in range(0, n_msgs - 1):
        first_msg = msgs[0][j]
        for i in range(1, n_peers - 1):
            if first_msg != msgs[i][j]:
                unordered = unordered + 1
                break
    return unordered


def full_compare(msgs):
    lengths = [len(log) for log in msgs]
    max_len = max(lengths) if lengths else 0
    unordered = 0
    divergent_positions = []
    for j in range(max_len):
        column = []
        for log in msgs:
            column.append(log[j] if j < len(log) else None)
        if any(item != column[0] for item in column[1:]):
            unordered += 1
            divergent_positions.append(j)
    return unordered, divergent_positions, lengths


def expected_ids(n_peers, n_msgs):
    return {(pid, seq) for pid in range(n_peers) for seq in range(n_msgs)}


def analyze_log(log, n_peers, n_msgs):
    expected = expected_ids(n_peers, n_msgs)
    ids = [tuple(item) for item in log]
    unique = set(ids)
    return {
        "length": len(ids),
        "expected_length": n_peers * n_msgs,
        "duplicates": len(ids) - len(unique),
        "missing": sorted(expected - unique),
        "unexpected": sorted(unique - expected),
    }


def scenario(name, logs, n_peers, n_msgs):
    original = original_compare(logs, n_peers, n_msgs)
    full, positions, lengths = full_compare(logs)
    details = [analyze_log(log, n_peers, n_msgs) for log in logs]
    print(f"SCENARIO {name}")
    print(f"  n_peers={n_peers} n_msgs={n_msgs} lengths={lengths}")
    print(f"  original_unordered={original}")
    print(f"  full_unordered={full} positions={positions}")
    print(f"  per_log={details}")
    print()
    return original, full


def main():
    # Last peer differs at every position; original range(1, N-1) skips it.
    logs_last_peer = [
        [(0, 0), (1, 0), (2, 0)],
        [(0, 0), (1, 0), (2, 0)],
        [(2, 0), (1, 0), (0, 0)],
    ]
    scenario("last-peer-diverges", logs_last_peer, 3, 3)

    # Only the last position diverges; original range(0, N_MSGS-1) skips it.
    logs_last_pos = [
        [(0, 0), (0, 1), (1, 0)],
        [(0, 0), (0, 1), (1, 1)],
        [(0, 0), (0, 1), (1, 1)],
    ]
    scenario("last-position-diverges", logs_last_pos, 3, 3)

    # Two peers: original inner loop is empty.
    logs_two = [
        [(0, 0), (1, 0)],
        [(1, 0), (0, 0)],
    ]
    scenario("two-peers-reordered", logs_two, 2, 2)

    # Realistic size: each log has N * nMsgs items; original looks at nMsgs-1 only.
    complete = [(pid, seq) for pid in range(3) for seq in range(5)]
    reordered = list(complete)
    reordered[-1], reordered[0] = reordered[0], reordered[-1]
    logs_full = [complete, complete, reordered]
    scenario("full-log-last-item-swapped", logs_full, 3, 5)

    out = pathlib.Path(__file__).resolve().parent / "originais" / "comparisonServer.py"
    source = out.read_text(encoding="utf-8")
    print("ORIGINAL_SOURCE_HAS_N_MSGS_MINUS_1", "range(0,N_MSGS-1)" in source.replace(" ", ""))
    print("ORIGINAL_SOURCE_HAS_N_MINUS_1", "range(1,N-1)" in source.replace(" ", ""))


if __name__ == "__main__":
    main()
