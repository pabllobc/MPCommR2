"""Validate complete arrival and delivery logs collected from ordered peers."""

import ast
from pathlib import Path
import sys


def read_logs(directory, prefix):
    """Return peer-indexed literal logs for one evidence-file prefix."""
    logs = {}
    for path in directory.glob(f"{prefix}-peer-*.log"):
        peer_id = int(path.stem.rsplit("-", 1)[1])
        logs[peer_id] = ast.literal_eval(path.read_text(encoding="utf-8"))
    return logs


def validate_sets(logs, peer_ids, expected):
    """Require every peer log to contain exactly the expected identifiers."""
    if set(logs) != peer_ids:
        raise AssertionError(
            f"Expected log peers {sorted(peer_ids)}, found {sorted(logs)}"
        )
    for peer_id in sorted(peer_ids):
        entries = [tuple(item) for item in logs[peer_id]]
        unique = set(entries)
        missing = sorted(expected - unique)
        unexpected = sorted(unique - expected)
        duplicates = len(entries) - len(unique)
        print(
            f"Peer {peer_id}: length={len(entries)} "
            f"duplicates={duplicates} missing={missing} "
            f"unexpected={unexpected}"
        )
        if (
            len(entries) != len(expected)
            or duplicates
            or missing
            or unexpected
        ):
            raise AssertionError(f"Peer {peer_id} has an invalid message set")


def main():
    """Validate CLI-selected evidence and print the full common delivery."""
    if len(sys.argv) != 4:
        print(
            "Usage: python validate_ordered_logs.py "
            "<evidence-directory> <n-peers> <messages-per-peer>"
        )
        raise SystemExit(2)

    directory = Path(sys.argv[1])
    n_peers = int(sys.argv[2])
    n_msgs = int(sys.argv[3])
    peer_ids = set(range(n_peers))
    expected = {
        (peer_id, sequence)
        for peer_id in peer_ids
        for sequence in range(n_msgs)
    }

    print("Arrival logs:")
    validate_sets(read_logs(directory, "arrival"), peer_ids, expected)

    print("Delivery logs:")
    deliveries = read_logs(directory, "delivery")
    validate_sets(deliveries, peer_ids, expected)
    first = deliveries[0]
    if any(deliveries[peer_id] != first for peer_id in sorted(peer_ids)[1:]):
        raise AssertionError("Delivery sequences are not identical")

    print("Delivery sequences identical: True")
    print(f"Verified peers: {n_peers}")
    print(f"Messages per peer: {n_msgs}")
    print(f"Delivered messages per peer: {len(first)}")
    print(f"Totally ordered delivery: {first}")


if __name__ == "__main__":
    main()
