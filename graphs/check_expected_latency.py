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


# A stall is a discontinuity in the run's latency profile: one window is slower than the
# windows either side of it. Offered load alone never does that -- a deployment at its capacity
# limit ramps gradually -- so an abrupt step means something outside the protocol changed state
# mid-run, and the window is no longer comparable to its neighbours.
#
# The comparison is the low end of the suspect window against a *higher* quantile of its
# neighbours, because a stall lifts the whole distribution while a mode shift only moves the
# middle. Comparing like quantiles on both sides cannot tell those apart: any bi- or
# multi-stable latency -- the 50:50 read/write mix, or a fast path against a slow one -- steps
# between two stable values as the mix drifts, and a median-to-median test reads that as a
# stall. Requiring the window's 20th percentile to exceed twice its neighbours' 40th means even
# its fastest fifth is slower than much of theirs, which a mode shift does not achieve.
#
# Neither recovery nor duration is required and no window is excluded: a stall in the last
# twentieth of a run is as good a reason to discard it as one in the middle, and requiring a
# return to baseline would systematically miss exactly those.
# Kept distinct from the 1 that a latency-expectation violation exits with, and from the 2
# the runner treats as "the checker itself broke".
STALL_EXIT_CODE = 3
STALL_WINDOWS = 20
STALL_WINDOW_QUANTILE = 5
STALL_NEIGHBOUR_QUANTILE = 40
STALL_FACTOR = 3.0
STALL_MIN_SAMPLES = 200


def quantile(sorted_values: list[float], percent: int) -> float:
    index = percent * len(sorted_values) // 100
    return sorted_values[min(index, len(sorted_values) - 1)]


def is_target_experiment(path: Path, log_dir: Path) -> bool:
    parts = path.relative_to(log_dir).parts or path.parts
    return "t=1000" in parts and "conflicts=false" in parts


def is_faults(path: Path, log_dir: Path) -> bool:
    parts = path.relative_to(log_dir).parts or path.parts
    return "f=" not in parts


def stall_ratio(samples: list[tuple[int, float, str]]) -> float | None:
    """How far the worst window stands out from its neighbours, or `None` if none does.

    Requests carry a per-client `request_id` that increases with time, so ordering by it and
    cutting into equal windows recovers the run's profile without needing a timestamp. Issue
    order rather than completion order is what keeps the final window unbiased: the requests
    issued last still have the sustain phase to finish in.

    A neighbour is compared at its own quantile rather than pooled, and the *lower* of the two
    is used, so that a stall spanning several windows is still caught at its edge, where one
    side is undisturbed.
    """
    worst = None
    for operation in ("Get", "Put"):
        latencies = [latency for _, latency, op in sorted(samples) if op == operation]
        if len(latencies) < STALL_MIN_SAMPLES:
            continue
        count = len(latencies)
        windows = [
            sorted(latencies[i * count // STALL_WINDOWS : (i + 1) * count // STALL_WINDOWS])
            for i in range(STALL_WINDOWS)
        ]
        low = [quantile(window, STALL_WINDOW_QUANTILE) for window in windows]
        neighbour = [quantile(window, STALL_NEIGHBOUR_QUANTILE) for window in windows]
        for index in range(STALL_WINDOWS):
            around = [
                neighbour[side]
                for side in (index - 1, index + 1)
                if 0 <= side < STALL_WINDOWS
            ]
            calmest = min(around)
            if calmest <= 0:
                continue
            ratio = low[index] / calmest
            if ratio > STALL_FACTOR and (worst is None or ratio > worst):
                worst = ratio
    return worst


def discover_process_logs(log_dir: Path) -> dict[Path, dict[int, Path]]:
    """Return the newest available log for each process."""
    experiments: dict[Path, dict[int, Path]] = defaultdict(dict)

    for path in log_dir.rglob("*.stdout"):
        match = STDOUT_RE.fullmatch(path.name)
        if match:
            experiments[path.parent][int(match.group("pid"))] = path

    for path in log_dir.rglob("server_*.log"):
        match = SERVER_LOG_RE.fullmatch(path.name)
        if match:
            pid = int(match.group("pid"))
            current = experiments[path.parent].get(pid)
            if current is None or path.stat().st_mtime_ns > current.stat().st_mtime_ns:
                experiments[path.parent][pid] = path

    return experiments


def parse_process_log(
    path: Path,
) -> tuple[float | None, float | None, float | None, float | None, float | None]:
    executed_latencies_ms = []
    profile_samples: list[tuple[int, float, str]] = []
    expected_ms = None

    with path.open(encoding="utf-8", errors="replace") as log:
        for line in log:
            if line.startswith(EXECUTED_PREFIX):
                try:
                    payload = json.loads(line.split(" | ", maxsplit=1)[1])
                    latency_ms = duration_to_ms(payload["latency"])
                    executed_latencies_ms.append(latency_ms)
                    response = payload["response"]
                    operation = "Get" if response.get("Get") else "Put"
                    profile_samples.append(
                        (response[operation]["request_id"], latency_ms, operation)
                    )
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
    return avg_ms, p95_ms, p99_ms, expected_ms, stall_ratio(profile_samples)


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

    stalls: dict[Path, list[tuple[int, float]]] = defaultdict(list)

    for experiment in sorted(experiments.keys()):
        process_logs = experiments[experiment]
        expected_applies = is_target_experiment(experiment, log_dir)
        for pid, path in process_logs.items():
            try:
                avg_ms, p95_ms, p99_ms, expected_ms, stall = parse_process_log(path)
            except OSError as error:
                print(f"warning: could not read {path}: {error}", file=sys.stderr)
                incomplete += 1
                continue

            if stall is not None:
                stalls[experiment].append((pid, stall))

            if not expected_applies:
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

        if experiment in stalls:
            print(experiment.relative_to(log_dir.parent))
            for pid, ratio in sorted(stalls[experiment]):
                print(
                    f"  process {pid:>2}: one window of the run was {ratio:.1f}x slower than"
                    f" the windows around it. That is a stall, not saturation; rerun."
                )

        if experiment not in violations:
            if experiment not in stalls:
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
        f"found {len(violations)} experiments with violations and {len(stalls)} with a "
        f"mid-run stall; skipped {incomplete} incomplete process logs."
    )
    # A stall is an artifact of the measurement rather than a property of the run, so the
    # runner distinguishes it: it is worth more attempts than a run that is merely slower
    # than it should be. Reported even when there are also latency violations, since the
    # stall is the one that says another attempt is likely to look different.
    if stalls:
        return STALL_EXIT_CODE
    return 1 if violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
