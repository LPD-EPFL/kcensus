"""The exp-5 grid and log access, shared so the two plot scripts cannot disagree on it."""
import sys
from dataclasses import dataclass
from math import ceil
from statistics import median

from common import ALGORITHMS, args
from logparser import duration_to_ms, parse

EXP5_CONFIG = "aws-ring-7"
EXP5_DURATION = "10s"
EXP5_CDF_DURATION = "10s"
EXP5_INGRESS = "exponential"
EXP5_ALGORITHMS = ALGORITHMS
LOAD_EDGE_FRACTION = 0.1

WRITES = 0.5
KEYS = 100000
CDF_THROUGHPUT = 1000
SKEWS = (0.0, 0.8, 0.99)
LOAD_SKEWS = (0.0, 0.8, 0.99)
# Keep this in sync with EXP5_THROUGHPUTS in lib.sh. Using the configured ladder, rather than
# discovering only successful runs on disk, lets Figure 14 stop each curve at the first
# unsustained point.
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
    latencies: list[float]
    sustained: bool
    failure_reason: str | None = None


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


def _latency_drift_failure(logs, proposer_count):
    """Why a run's final latency edge has drifted above its initial edge, if it has."""
    for pid in range(proposer_count):
        items = logs["executed"].get(pid, [])
        if not items:
            return f"proposer {pid} has no measured requests"
        latencies = [duration_to_ms(item["latency"]) for item in items]
        edge_size = max(1, ceil(len(latencies) * LOAD_EDGE_FRACTION))
        initial_median = median(latencies[:edge_size])
        final_minimum = min(latencies[-edge_size:])
        if final_minimum > initial_median:
            return (
                f"proposer {pid} final 10% minimum {final_minimum:.1f}ms exceeds "
                f"initial 10% median {initial_median:.1f}ms"
            )
    return None


def run_stats(
    algo,
    workload,
    throughput,
    duration=EXP5_DURATION,
    reject_latency_drift=False,
):
    """Write latencies and run-quality status, or `None` if logs are absent.

    A proposer emits `client-done` only after every request it scheduled has received a response.
    Requiring that marker from all seven proposers therefore distinguishes a completed run from
    the partial logs left by a timed-out, unsustained run. Load plots additionally reject a run
    when any proposer's final 10% of latencies has drifted entirely above its initial 10%.
    """
    try:
        logs = parse(
            config=EXP5_CONFIG,
            algo=algo,
            writes=WRITES,
            duration=duration,
            ingress=EXP5_INGRESS,
            throughput=throughput,
            speedup=1,
            faults="",
            keys=workload.keys,
            skew=workload.skew,
            shards=workload.keys,
            conflicts="conflicts=true",
        )
    except FileNotFoundError:
        return None
    latencies = [
        duration_to_ms(log["latency"])
        for items in logs["executed"].values()
        for log in items
        if log["response"].get("Put") is not None
    ]
    proposer_count = int("".join(char for char in EXP5_CONFIG if char.isdigit()))
    failure_reason = None
    if not all(logs["client-done"].get(pid) for pid in range(proposer_count)):
        failure_reason = "not every proposer completed"
    elif reject_latency_drift:
        failure_reason = _latency_drift_failure(logs, proposer_count)
    return RunStats(
        latencies,
        sustained=failure_reason is None,
        failure_reason=failure_reason,
    )
