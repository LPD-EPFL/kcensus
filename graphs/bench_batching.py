#!/usr/bin/env python3
"""Tables for a `bench-batching.sh` tree: one row per (variant, setting).

The sweep writes `<out>/<variant>/<the usual log path>`, so a run is found by its `.done`
marker rather than by rebuilding a path, and the settings are read back out of the path.

  ./bench_batching.py                  # every run under ../bench-logs
  ./bench_batching.py ../bench-logs E4 # one variant
  ./bench_batching.py ../bench-logs ring  # settings whose config matches

Percentiles come from `logparser`, so they are the ones the figures use.
"""
import sys
from pathlib import Path

from logparser import compute_percentiles, duration_to_ms, parse_file

# What a run reports, beyond the latencies.
NETWORK = "network-done"
RESOURCES = "time"


def read_run(directory):
    """`(latencies_ms, executed, messages, bytes, peak_rss_kb, completed)` for one run.

    `completed` is false when any replica panicked, which is how the sweep records a rung
    the deployment could not sustain.
    """
    latencies, executed, messages, byte_count, peak_rss = [], 0, 0, 0, 0
    completed = True
    for stdout in sorted(directory.glob("*.stdout")):
        stderr = stdout.with_suffix(".stderr")
        if stderr.exists() and "panicked" in stderr.read_text(errors="replace"):
            completed = False
        with stdout.open(errors="replace") as file:
            logs = parse_file(file)
        with stderr.open(errors="replace") as file:
            resources = parse_file(file)
        for event in logs[NETWORK]:
            messages += event["msg_count"]
            byte_count += event["byte_count"]
        for event in resources[RESOURCES]:
            peak_rss = max(peak_rss, event["memory"])
        for event in logs["executed"]:
            executed += 1
            latencies.append(duration_to_ms(event["latency"]))
    return latencies, executed, messages, byte_count, peak_rss, completed


def settings_of(relative):
    """The variant and the `k=v` path components the sweep ran with."""
    variant = relative.parts[0]
    fields = dict(
        part.split("=", 1) for part in relative.parts[1:] if "=" in part
    )
    return variant, fields


def collect(root):
    rows = []
    for marker in root.rglob(".done"):
        run = marker.parent
        variant, fields = settings_of(run.relative_to(root))
        latencies, executed, messages, byte_count, peak_rss, completed = read_run(run)
        percentiles = compute_percentiles(latencies) if latencies else [0] * 101
        duration = int(fields.get("d", "10s").rstrip("s")) or 1
        rows.append(
            {
                "variant": variant,
                "config": fields.get("c", "?"),
                "skew": fields.get("skew", "?"),
                "offered": int(fields.get("t", 0)),
                "achieved": executed / duration,
                "avg": sum(latencies) / len(latencies) if latencies else 0.0,
                "p50": percentiles[50],
                "p90": percentiles[90],
                "p95": percentiles[95],
                "p99": percentiles[99],
                "messages": messages,
                "megabytes": byte_count / 1e6,
                "rss": peak_rss / 1024.0,
                "completed": completed,
            }
        )
    return rows


HEADER = (
    f"{'variant':7} {'config':22} {'skew':>5} {'offered':>8} {'achieved':>9} "
    f"{'avg(ms)':>8} {'p50(ms)':>8} {'p90(ms)':>8} {'p95(ms)':>8} {'p99(ms)':>8} {'msgs':>10} {'MB':>8} {'RSS(MB)':>8}"
)


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "../bench-logs")
    wanted = sys.argv[2] if len(sys.argv) > 2 else ""
    rows = collect(root)
    if not rows:
        raise SystemExit(f"no completed runs under {root}")
    rows.sort(key=lambda row: (row["config"], row["skew"], row["offered"], row["variant"]))

    group = None
    for row in rows:
        if wanted and wanted not in row["config"] and wanted != row["variant"]:
            continue
        if (row["config"], row["skew"], row["offered"]) != group:
            group = (row["config"], row["skew"], row["offered"])
            print()
            print(HEADER)
            print("-" * len(HEADER))
        print(
            f"{row['variant']:7} {row['config'][:22]:22} {row['skew']:>5} "
            f"{row['offered']:8} {row['achieved']:9.0f} {row['avg']:8.1f} "
            f"{row['p50']:8.1f} {row['p90']:8.1f} {row['p95']:8.1f} "
            f"{row['p99']:8.1f} {row['messages']:10} {row['megabytes']:8.1f} "
            f"{row['rss']:8.1f}" + ("" if row["completed"] else "  NOT SUSTAINED")
        )


if __name__ == "__main__":
    main()
