"""The exp-5 grid and log access, shared so the two plot scripts cannot disagree on it."""
import sys
from dataclasses import dataclass
from math import ceil
from statistics import median

import logparser
from common import ALGORITHMS, args
from logparser import duration_to_ms, parse

EXP5_CONFIG = "aws-ring-7"
EXP5_DURATION = "10s"
EXP5_CDF_DURATION = "10s"
EXP5_INGRESS = "exponential"
EXP5_ALGORITHMS = ALGORITHMS
LOAD_EDGE_FRACTION = 0.1
LOAD_DURATION_SECONDS = 10.0

WRITES = 0.5
KEYS = 100000
CDF_THROUGHPUT = 1000
SKEWS = (0.0, 0.99)
LOAD_SKEWS = (0.0, 0.99)
# Keep this in sync with EXP5_THROUGHPUTS in lib.sh. Using the configured ladder, rather than
# discovering only successful runs on disk, lets Figure 14 estimate achieved throughput for
# every configured load rung.
THROUGHPUTS = (
    500,
    1000,
    *range(2000, 20000, 2000),
    *range(20000, 50000, 5000),
)


@dataclass(frozen=True)
class Workload:
    """One column of the sweep."""

    skew: float
    keys: int = KEYS

    @property
    def slug(self) -> str:
        return f"skew{self.skew:g}-k{self.keys}"

    @property
    def label(self) -> str:
        return f"Zipf {self.skew:g}" if self.skew > 0 else "Uniform"

    @property
    def latency_xlim(self) -> float:
        """Where to cut the CDF, per skew."""
        # return {0.0: 340, 0.5: 390, 0.8: 440, 0.99: 490}[self.skew]
        return 450


@dataclass(frozen=True)
class RunStats:
    put_latencies: list[float]
    read_latencies: list[float]
    sustained: bool
    failure_reason: str | None = None
    aborted: bool = False
    estimated_throughput: float | None = None
    from_failed_logs: bool = False

    @property
    def latencies(self) -> list[float]:
        """Put latencies, retained as the default latency series."""
        return self.put_latencies


CDF_WORKLOADS = tuple(
    Workload(skew, KEYS) for skew in SKEWS
)
LOAD_WORKLOADS = tuple(
    Workload(skew, KEYS) for skew in LOAD_SKEWS
)


def _flag_given(*names):
    """Whether the flag was passed. `common` parses at import and supplies defaults, so the
    parsed value cannot say -- and it rejects flags of our own."""
    for arg in sys.argv[1:]:
        for name in names:
            if arg == name or arg.startswith(name + "="):
                return True
    return False


def selected_throughput():
    """The rung the CDFs are cut at. `common`'s `-t` default of 10 is not on the ladder."""
    if _flag_given("-t", "--throughput"):
        return args.throughput
    return CDF_THROUGHPUT


def selected_workloads(candidates):
    """All of them, unless `--skew` narrows it."""
    if not _flag_given("--skew"):
        return list(candidates)
    chosen = [w for w in candidates if w.skew == args.skew]
    if not chosen:
        raise SystemExit(
            f"--skew {args.skew:g} is not one of "
            + ", ".join(f"{s:g}" for s in sorted({w.skew for w in candidates}))
        )
    return chosen


def _estimated_achieved_throughput(logs, proposer_count, target_throughput):
    """Estimate achieved throughput from measured request count and tail latency growth."""
    request_counts = []
    initial_latencies = []
    final_latencies = []
    for pid in range(proposer_count):
        items = logs["executed"].get(pid, [])
        request_counts.append(len(items))
        if not items:
            continue
        latencies = [duration_to_ms(item["latency"]) for item in items]
        edge_size = max(1, ceil(len(latencies) * LOAD_EDGE_FRACTION))
        initial_latencies.extend(latencies[:edge_size])
        final_latencies.extend(latencies[-edge_size:])

    total_requests = sum(request_counts)
    if not total_requests or not initial_latencies or target_throughput <= 0:
        return None

    expected_per_proposer = (
        target_throughput * LOAD_DURATION_SECONDS / proposer_count
    )
    request_ratio = (
        sum(count / expected_per_proposer for count in request_counts) / proposer_count
    )
    latency_delta_seconds = (
        median(final_latencies) - median(initial_latencies)
    ) / 1000
    estimated_duration = (
        request_ratio * LOAD_DURATION_SECONDS + latency_delta_seconds
    )
    if estimated_duration <= 0:
        return None
    return total_requests / estimated_duration


def run_stats(
    algo,
    workload,
    throughput,
    duration=EXP5_DURATION,
    fallback_to_failed=False,
):
    """Put/read latencies, completion status, and estimated achieved throughput.

    A proposer emits `client-done` only after every request it scheduled has received a response.
    Requiring that marker from all seven proposers therefore distinguishes a completed run from
    the partial logs left by a timed-out run. The throughput estimate is available for both.
    """
    parse_args = {
        "config": EXP5_CONFIG,
        "algo": algo,
        "writes": WRITES,
        "duration": duration,
        "ingress": EXP5_INGRESS,
        "throughput": throughput,
        "speedup": 1,
        "faults": "",
        "keys": workload.keys,
        "skew": workload.skew,
        "shards": workload.keys,
        "conflicts": "conflicts=true",
    }
    logs = None
    try:
        logs = parse(**parse_args)
    except FileNotFoundError:
        pass

    from_failed_logs = False
    has_measured_requests = logs is not None and any(
        logs.get("executed", {}).values()
    )
    if fallback_to_failed and not has_measured_requests:
        try:
            logs = parse(
                **parse_args,
                log_dir=f"{logparser.LOG_DIR}/failed",
                attempt="latest",
            )
            from_failed_logs = True
        except FileNotFoundError:
            pass
    if logs is None:
        return None
    put_latencies = [
        duration_to_ms(log["latency"])
        for items in logs["executed"].values()
        for log in items
        if log["response"].get("Put") is not None
    ]
    read_latencies = [
        duration_to_ms(log["latency"])
        for items in logs["executed"].values()
        for log in items
        if log["response"].get("Get") is not None
    ]
    proposer_count = int("".join(char for char in EXP5_CONFIG if char.isdigit()))
    aborted = not all(logs["client-done"].get(pid) for pid in range(proposer_count))
    failure_reason = "not every proposer completed" if aborted else None
    estimated_throughput = _estimated_achieved_throughput(
        logs, proposer_count, throughput
    )
    return RunStats(
        put_latencies,
        read_latencies,
        sustained=failure_reason is None,
        failure_reason=failure_reason,
        aborted=aborted,
        estimated_throughput=estimated_throughput,
        from_failed_logs=from_failed_logs,
    )
