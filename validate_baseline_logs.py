"""Validate MPComm logfile*.log files independently of comparisonServer."""

from __future__ import annotations

import ast
import pathlib
import sys


def expected_ids(n_peers: int, n_msgs: int):
    return {(pid, seq) for pid in range(n_peers) for seq in range(n_msgs)}


def analyze_log(log, n_peers: int, n_msgs: int):
    ids = [tuple(item) for item in log]
    unique = set(ids)
    expected = expected_ids(n_peers, n_msgs)
    return {
        "length": len(ids),
        "expected_length": n_peers * n_msgs,
        "duplicates": len(ids) - len(unique),
        "missing": sorted(expected - unique),
        "unexpected": sorted(unique - expected),
        "sequence": ids,
    }


def compare_all(logs):
    unordered = 0
    divergent = []
    max_len = max((len(log) for log in logs), default=0)
    for index in range(max_len):
        column = [log[index] if index < len(log) else None for log in logs]
        if any(item != column[0] for item in column[1:]):
            unordered += 1
            divergent.append(index)
    return unordered, divergent


def main():
    if len(sys.argv) < 4:
        print("Usage: python validate_baseline_logs.py <dir> <n_peers> <n_msgs>")
        sys.exit(2)
    directory = pathlib.Path(sys.argv[1])
    n_peers = int(sys.argv[2])
    n_msgs = int(sys.argv[3])
    logs = []
    for pid in range(n_peers):
        path = directory / f"logfile{pid}.log"
        if not path.exists():
            print(f"MISSING {path}")
            sys.exit(1)
        log = ast.literal_eval(path.read_text(encoding="utf-8"))
        report = analyze_log(log, n_peers, n_msgs)
        logs.append(report["sequence"])
        print(f"logfile{pid}.log length={report['length']} expected={report['expected_length']} "
              f"duplicates={report['duplicates']} missing={report['missing']} unexpected={report['unexpected']}")
        print(f"  sequence={report['sequence']}")
    identical = all(log == logs[0] for log in logs[1:]) if logs else True
    unordered, divergent = compare_all(logs)
    print(f"sequences_identical={identical}")
    print(f"unordered_rounds={unordered}")
    print(f"divergent_positions={divergent}")
    complete = all(
        len(log) == n_peers * n_msgs and len(set(log)) == n_peers * n_msgs
        for log in logs
    )
    print(f"complete_identifier_sets={complete}")
    sys.exit(0 if complete else 1)


if __name__ == "__main__":
    main()
