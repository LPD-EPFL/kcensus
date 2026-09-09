#!/usr/bin/env python3
"""Report low-contention runs whose measured latency exceeds their expectation."""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from logparser import compute_average, compute_percentiles, duration_to_ms


EXECUTED_PREFIX = "[log=executed]"
EXPECTED_RE = re.compile(
    r"Expected local latency \(no-contention\):\s*"
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ns|[µμu]s|ms|s)"
)
STDOUT_RE = re.compile(r"(?P<pid>[0-9]+)\.stdout$")
SERVER_LOG_RE = re.compile(r"server_(?P<pid>[0-9]+)\.log$")
UNIT_TO_MS = {
    "ns": 1e-6,
    "us": 1e-3,
    "µs": 1e-3,
    "μs": 1e-3,
    "ms": 1.0,
    "s": 1_000.0,
}


def is_target_experiment(path: Path, log_dir: Path) -> bool:
    parts = path.relative_to(log_dir).parts or path.parts
    return "t=1000" in parts and "conflicts=false" in parts

def is_faults(path: Path, log_dir: Path) -> bool:
    parts = path.relative_to(log_dir).parts or path.parts
    return "f=" not in parts


def discover_process_logs(log_dir: Path) -> dict[Path, dict[int, Path]]:
    """Return the newest available log for each process."""
    experiments: dict[Path, dict[int, Path]] = defaultdict(dict)

    for path in log_dir.rglob("*.stdout"):
        match = STDOUT_RE.fullmatch(path.name)
        if match and is_target_experiment(path.parent, log_dir):
            experiments[path.parent][int(match.group("pid"))] = path

    for path in log_dir.rglob("server_*.log"):
        match = SERVER_LOG_RE.fullmatch(path.name)
        if match and is_target_experiment(path.parent, log_dir):
            pid = int(match.group("pid"))
            current = experiments[path.parent].get(pid)
            if current is None or path.stat().st_mtime_ns > current.stat().st_mtime_ns:
                experiments[path.parent][pid] = path

    return experiments


def parse_process_log(
    path: Path,
) -> tuple[float | None, float | None, float | None, float | None]:
    executed_latencies_ms = []
    expected_ms = None

    with path.open(encoding="utf-8", errors="replace") as log:
        for line in log:
            if line.startswith(EXECUTED_PREFIX):
                try:
                    payload = json.loads(line.split(" | ", maxsplit=1)[1])
                    executed_latencies_ms.append(duration_to_ms(payload["latency"]))
                except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                    pass

            expected_match = EXPECTED_RE.search(line)
            if expected_match:
                expected_ms = (
                    float(expected_match.group("value"))
                    * UNIT_TO_MS[expected_match.group("unit")]
                )

    avg_ms = (
        compute_average(executed_latencies_ms) if executed_latencies_ms else None
    )
    percentiles = (
        compute_percentiles(executed_latencies_ms)
        if executed_latencies_ms
        else None
    )
    p95_ms = percentiles[95] if percentiles else None
    p99_ms = percentiles[99] if percentiles else None
    return avg_ms, p95_ms, p99_ms, expected_ms


def main() -> int:
    default_log_dir = Path(__file__).resolve().parents[1] / "logs"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "log_dir",
        nargs="?",
        type=Path,
        default=default_log_dir,
        help=f"log directory to scan (default: {default_log_dir})",
    )
    args = parser.parse_args()
    log_dir = args.log_dir.expanduser()

    if not log_dir.is_dir():
        parser.error(f"not a directory: {log_dir}")

    experiments = discover_process_logs(log_dir)
    violations: dict[
        Path, list[tuple[int, float, float, float, float, float, float, float]]
    ] = defaultdict(list)
    checked = 0
    incomplete = 0

    for experiment in sorted(experiments.keys()):
        process_logs = experiments[experiment]
        for pid, path in process_logs.items():
            try:
                avg_ms, p95_ms, p99_ms, expected_ms = parse_process_log(path)
            except OSError as error:
                print(f"warning: could not read {path}: {error}", file=sys.stderr)
                incomplete += 1
                continue

            if avg_ms is None or p95_ms is None or p99_ms is None or expected_ms is None:
                incomplete += 1
                continue

            checked += 1
            # message load due to process count, from 0 for n<=7, to 1 for n=31
            load_factor = (len(process_logs)-7)/24 if len(process_logs) > 7 else 0
            if is_faults(experiment, log_dir):
                # Many faults are aggregated, so noise is less of an issue
                assert load_factor == 0, "exp-2 runs on 7 nodes"
                load_factor += 0.5
            avg_factor = 1.1 + 0.3*load_factor # avg tolerance from 1.1x to 1.4x
            p95_factor = 1.3 + 0.3*load_factor # p95 tolerance from 1.3x to 1.6x
            p99_factor = 2.2 + 3.0*load_factor # p99 tolerance from 2.2x to 5.2x
            avg_limit = avg_factor * expected_ms + 1.0
            p95_limit = p95_factor * expected_ms + 1.0
            p99_limit = p99_factor * expected_ms + 1.0
            if avg_ms > avg_limit or p95_ms > p95_limit or p99_ms > p99_limit:
                violations[experiment].append(
                    (pid, expected_ms, avg_ms, avg_limit, p95_ms, p95_limit, p99_ms, p99_limit)
                )

        if experiment not in violations:
            print(experiment.relative_to(log_dir.parent), "is clean !")
            continue
        print(experiment.relative_to(log_dir.parent))
        for pid, expected_ms, avg_ms, avg_limit, p95_ms, p95_limit, p99_ms, p99_limit  in sorted(
            violations[experiment]
        ):
            print(
                f"  process {pid:>2}: expected={expected_ms:>7.3f}ms avg={avg_ms:>7.3f}ms"
                f" p95={p95_ms:>7.3f}ms p99={p99_ms:>7.3f}ms:"
            )
            if avg_ms > avg_limit:
                print(
                    f"    - avg too high: {avg_ms:>7.3f}ms > {avg_limit:>7.3f}ms"
                    f" (avg is {100*avg_ms/expected_ms:2f}% of expected)"
                )
            if p95_ms > p95_limit:
                print(
                    f"    - p95 too high: {p95_ms:>7.3f}ms > {p95_limit:>7.3f}ms"
                    f" (p95 is {100*p95_ms/expected_ms:2f}% of expected)"
                )
            if p99_ms > p99_limit:
                print(
                    f"    - p99 too high: {p99_ms:>7.3f}ms > {p99_limit:>7.3f}ms"
                    f" (p99 is {100*p99_ms/expected_ms:2f}% of expected)"
                )

    print(
        f"Checked {checked} process logs in {len(experiments)} experiments; "
        f"found {len(violations)} experiments with violations; "
        f"skipped {incomplete} incomplete process logs."
    )
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
