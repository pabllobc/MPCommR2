"""Run comparable local benchmarks for baseline and ordered MPComm."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import statistics
import time
from collections import Counter
from pathlib import Path

import run_baseline_local
import run_ordered_local
from validate_baseline_logs import analyze_log, compare_all


ROOT = Path(__file__).resolve().parent
DEFAULT_BINDS = ("127.0.0.1", "127.0.0.2", "127.0.0.3")


def read_literal(path: Path):
    """Read one Python literal evidence file produced by MPComm peers."""
    return ast.literal_eval(path.read_text(encoding="utf-8"))


def expected_ids(n_peers, n_msgs):
    """Return the full set delivered or observed by each receiver."""
    return {
        (peer_id, sequence)
        for peer_id in range(n_peers)
        for sequence in range(n_msgs)
    }


def validate_baseline(run_dir, n_peers, n_msgs):
    """Validate baseline logfile outputs and return comparison metrics."""
    logs = []
    valid = True
    for peer_id in range(n_peers):
        path = run_dir / f"logfile{peer_id}.log"
        if not path.exists():
            valid = False
            continue
        log = read_literal(path)
        report = analyze_log(log, n_peers, n_msgs)
        logs.append(report["sequence"])
        valid = valid and (
            report["length"] == report["expected_length"]
            and report["duplicates"] == 0
            and not report["missing"]
            and not report["unexpected"]
        )
    identical = all(log == logs[0] for log in logs[1:]) if logs else False
    unordered, divergent = compare_all(logs) if logs else (0, [])
    return {
        "valid": valid and len(logs) == n_peers,
        "sequences_identical": identical,
        "unordered_rounds": unordered,
        "divergent_positions": divergent,
        "delivered_per_peer": len(logs[0]) if logs else 0,
    }


def complete_log(log, expected):
    """Return whether a log contains exactly the expected identifiers once."""
    return (
        len(log) == len(expected)
        and len(set(log)) == len(expected)
        and set(log) == expected
    )


def read_peer_log(path):
    """Read one peer log as normalized tuple identifiers."""
    return [tuple(item) for item in read_literal(path)]


def validate_ordered(run_dir, n_peers, n_msgs):
    """Validate ordered arrival and delivery logs and count protocol events."""
    expected = expected_ids(n_peers, n_msgs)
    arrivals = {}
    deliveries = {}
    valid = True
    for peer_id in range(n_peers):
        arrival_path = run_dir / f"arrival-peer-{peer_id}.log"
        delivery_path = run_dir / f"delivery-peer-{peer_id}.log"
        if not arrival_path.exists() or not delivery_path.exists():
            valid = False
            continue
        arrivals[peer_id] = read_peer_log(arrival_path)
        deliveries[peer_id] = read_peer_log(delivery_path)
        valid = valid and complete_log(arrivals[peer_id], expected)
        valid = valid and complete_log(deliveries[peer_id], expected)

    first = deliveries.get(0, [])
    identical = (
        len(deliveries) == n_peers
        and all(deliveries[peer_id] == first for peer_id in range(1, n_peers))
    )
    event_counts = Counter()
    for peer_id in range(n_peers):
        path = run_dir / f"protocol-peer-{peer_id}.log"
        if not path.exists():
            continue
        protocol = read_literal(path)
        for event in protocol.get("engine", []):
            event_counts[event["event"]] += 1
        for event in protocol.get("transport", []):
            event_counts[event["event"]] += 1
    return {
        "valid": valid and identical,
        "sequences_identical": identical,
        "unordered_rounds": 0,
        "divergent_positions": [],
        "delivered_per_peer": len(first),
        "event_counts": dict(sorted(event_counts.items())),
    }


def run_once(variant, n_msgs, repetition, outdir, bind_addrs):
    """Run one benchmark repetition and return one raw-result record."""
    run_dir = outdir / "raw" / f"{variant}-k{n_msgs}-r{repetition}"
    started_ns = time.monotonic_ns()
    error = None
    result = None
    try:
        if variant == "baseline":
            result = run_baseline_local.run(n_msgs, run_dir, list(bind_addrs))
        elif variant == "ordered":
            result = run_ordered_local.run(n_msgs, run_dir, list(bind_addrs))
        else:
            raise ValueError(f"Unknown benchmark variant: {variant}")
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    ended_ns = time.monotonic_ns()

    n_peers = result["n_peers"] if result else len(bind_addrs)
    if error is None and variant == "baseline":
        validation = validate_baseline(run_dir, n_peers, n_msgs)
    elif error is None:
        validation = validate_ordered(run_dir, n_peers, n_msgs)
    else:
        validation = {
            "valid": False,
            "sequences_identical": False,
            "unordered_rounds": None,
            "divergent_positions": [],
            "delivered_per_peer": 0,
        }

    return {
        "variant": variant,
        "n_peers": n_peers,
        "n_msgs": n_msgs,
        "messages_per_peer": n_msgs,
        "expected_deliveries_per_peer": n_peers * n_msgs,
        "auto_receive": True,
        "repetition": repetition,
        "started_monotonic_ns": started_ns,
        "ended_monotonic_ns": ended_ns,
        "duration_ms": (ended_ns - started_ns) / 1_000_000,
        "success": error is None and validation["valid"],
        "error": error,
        **validation,
    }


def summarize(records):
    """Aggregate successful repetitions by variant and workload."""
    groups = {}
    for record in records:
        key = (record["variant"], record["n_msgs"])
        groups.setdefault(key, []).append(record)

    rows = []
    for (variant, n_msgs), items in sorted(groups.items()):
        durations = [
            item["duration_ms"] for item in items if item["success"]
        ]
        row = {
            "variant": variant,
            "n_msgs": n_msgs,
            "n_peers": items[0]["n_peers"],
            "expected_deliveries_per_peer": (
                items[0]["expected_deliveries_per_peer"]
            ),
            "repetitions": len(items),
            "successes": len(durations),
            "failures": len(items) - len(durations),
            "mean_ms": statistics.fmean(durations) if durations else None,
            "median_ms": statistics.median(durations) if durations else None,
            "stdev_ms": (
                statistics.stdev(durations) if len(durations) > 1 else 0.0
            ),
            "min_ms": min(durations) if durations else None,
            "max_ms": max(durations) if durations else None,
            "throughput_deliveries_per_s": None,
        }
        if row["mean_ms"]:
            total_deliveries = (
                row["n_peers"] * row["expected_deliveries_per_peer"]
            )
            row["throughput_deliveries_per_s"] = (
                total_deliveries / (row["mean_ms"] / 1000)
            )
        rows.append(row)
    return rows


def write_csv(path, rows):
    """Write rows with a stable union of all keys."""
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def svg_bar_chart(path, summary_rows):
    """Create a dependency-free SVG chart of mean duration with dispersion."""
    rows = [row for row in summary_rows if row["mean_ms"] is not None]
    width = 980
    height = 520
    margin = 70
    plot_width = width - (margin * 2)
    plot_height = height - 160
    max_value = max(row["max_ms"] for row in rows) if rows else 1
    step = plot_width / max(len(rows), 1)
    bar_width = min(44, step * 0.55)

    def scale(value):
        return margin + plot_height - (value / max_value * plot_height)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        (
            '<text x="70" y="35" font-size="20" font-family="Arial">'
            + 'Tempo medio de rodada completa por carga</text>'
        ),
        (
            '<text x="70" y="58" font-size="12" font-family="Arial">'
            + 'Fonte: benchmark_mpcomm.py, medicao local com time.monotonic_ns'
            + '</text>'
        ),
        f'<line x1="{margin}" y1="{margin}" x2="{margin}" '
        f'y2="{margin + plot_height}" stroke="black"/>',
        f'<line x1="{margin}" y1="{margin + plot_height}" '
        f'x2="{width - margin}" y2="{margin + plot_height}" '
        'stroke="black"/>',
        '<text x="15" y="230" font-size="12" font-family="Arial" '
        'transform="rotate(-90 15 230)">Duracao (ms)</text>',
        f'<text x="{width / 2 - 80}" y="{height - 25}" font-size="12" '
        'font-family="Arial">Carga: variante e K mensagens por peer</text>',
    ]
    for tick in range(6):
        value = max_value * tick / 5
        y = scale(value)
        parts.append(
            f'<line x1="{margin - 5}" y1="{y:.1f}" x2="{width - margin}" '
            f'y2="{y:.1f}" stroke="#dddddd"/>'
        )
        parts.append(
            f'<text x="18" y="{y + 4:.1f}" font-size="11" '
            f'font-family="Arial">{value:.0f}</text>'
        )

    for index, row in enumerate(rows):
        x_center = margin + (step * index) + (step / 2)
        y = scale(row["mean_ms"])
        h = margin + plot_height - y
        color = "#777777" if row["variant"] == "baseline" else "#222222"
        parts.append(
            f'<rect x="{x_center - bar_width / 2:.1f}" y="{y:.1f}" '
            f'width="{bar_width:.1f}" height="{h:.1f}" fill="{color}"/>'
        )
        low = max(row["mean_ms"] - row["stdev_ms"], 0)
        high = row["mean_ms"] + row["stdev_ms"]
        parts.append(
            f'<line x1="{x_center:.1f}" y1="{scale(low):.1f}" '
            f'x2="{x_center:.1f}" y2="{scale(high):.1f}" '
            'stroke="black"/>'
        )
        label = f'{row["variant"]}\\nK={row["n_msgs"]}'
        parts.append(
            f'<text x="{x_center:.1f}" y="{height - 85}" font-size="10" '
            f'font-family="Arial" text-anchor="middle">{label}</text>'
        )
        parts.append(
            f'<text x="{x_center:.1f}" y="{y - 7:.1f}" font-size="10" '
            f'font-family="Arial" text-anchor="middle">'
            f'{row["mean_ms"]:.0f}</text>'
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def parse_args():
    """Parse command-line options for reproducible local benchmark runs."""
    parser = argparse.ArgumentParser()
    parser.add_argument("outdir", type=Path)
    parser.add_argument("--messages", default="5,10,15")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--bind-addrs", nargs="*", default=DEFAULT_BINDS)
    return parser.parse_args()


def main():
    """Execute the full benchmark and persist raw and summarized evidence."""
    args = parse_args()
    args.outdir.mkdir(parents=True, exist_ok=True)
    messages = [int(item) for item in args.messages.split(",") if item]
    records = []
    for n_msgs in messages:
        for repetition in range(1, args.repetitions + 1):
            for variant in ("baseline", "ordered"):
                record = run_once(
                    variant,
                    n_msgs,
                    repetition,
                    args.outdir,
                    tuple(args.bind_addrs),
                )
                records.append(record)
                print(json.dumps(record, sort_keys=True))
                time.sleep(0.5)

    summary = summarize(records)
    (args.outdir / "benchmark-raw.jsonl").write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    (args.outdir / "benchmark-summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    write_csv(args.outdir / "benchmark-raw.csv", records)
    write_csv(args.outdir / "benchmark-summary.csv", summary)
    svg_bar_chart(args.outdir / "benchmark-duration.svg", summary)
    failed = [record for record in records if not record["success"]]
    if failed:
        raise SystemExit(
            f"{len(failed)} benchmark repetitions failed; see raw output"
        )


if __name__ == "__main__":
    main()
