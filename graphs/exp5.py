"""The exp-5 grid and log access, shared so the two plot scripts cannot disagree on it."""
import sys
from dataclasses import dataclass
from pathlib import Path
from math import ceil

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
# The slowest share of an edge, dropped before averaging it.
LOAD_DELTA_TRIM = 0.05

WRITES = 0.5
KEYS = 100000
CDF_THROUGHPUT = 1000
SKEWS = (0.0, 0.99)
LOAD_SKEWS = (0.0, 0.99)
def load_throughputs():
    """The rungs the sweep actually ran, read off the log tree.

    `exp-5-ladder` doubles the offered rate until one cannot be sustained and then refines
    around the last that could, so the rungs depend on where each algorithm turns over and
    are not known ahead of the run. The failed tree is included so that a rung past the wall
    still contributes its achieved throughput to Figure 14.
    """
    pattern = f"c={EXP5_CONFIG}/a=*/w={WRITES:g}/d={EXP5_DURATION}/i={EXP5_INGRESS}/t=*"
    rungs = set()
    for root in (logparser.LOG_DIR, f"{logparser.LOG_DIR}/failed"):
        for path in Path(root).glob(pattern):
            try:
                rungs.add(int(path.name.removeprefix("t=")))
            except ValueError:
                continue
    return tuple(sorted(rungs))


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


def _request_id(item):
    """The proposer's own sequence number for a request, or `None` if the shape is unexpected."""
    response = item["response"]
    for operation in ("Put", "Get"):
        if operation in response:
            return response[operation]["request_id"]
    return None


def _edge_latency(latencies):
    """Mean latency of an edge, once its slowest `LOAD_DELTA_TRIM` are dropped.

    An edge is a tenth of a run, so a handful of outliers move a mean a long way; trimming the
    top of it leaves the growth between the two edges rather than the worst run of either.
    """
    ordered = sorted(latencies)
    kept = len(ordered) - int(len(ordered) * LOAD_DELTA_TRIM)
    ordered = ordered[:kept] or ordered
    return sum(ordered) / len(ordered)


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
        # A proposer logs in completion order, so a slow request lands after faster ones issued
        # later. The edges are meant to be the run's first and last tenth as offered.
        ordered = sorted(
            (item for item in items if _request_id(item) is not None), key=_request_id
        )
        latencies = [duration_to_ms(item["latency"]) for item in ordered]
        if not latencies:
            continue
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
    # Never negative: the estimate is `target / (1 + delta / ...)`, so a negative delta would
    # report a run as achieving more than was offered it.
    latency_delta_seconds = max(
        0.0, _edge_latency(final_latencies) - _edge_latency(initial_latencies)
    ) / 1000
    # The edges are centred on the run's 5% and 95% marks, so the growth between them spans
    # `1 - LOAD_EDGE_FRACTION` of the window rather than all of it.
    estimated_duration = request_ratio * LOAD_DURATION_SECONDS + (
        latency_delta_seconds / (1 - LOAD_EDGE_FRACTION)
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
    the partial logs left by a timed-out run. Only a completed run is given a throughput
    estimate.
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
    # A killed run is truncated at both ends of the estimate: its request count stops wherever
    # the deadline fell, and the highest request ids holding a response are the ones that beat
    # it, which reads as latency falling over the run.
    estimated_throughput = (
        None
        if aborted
        else _estimated_achieved_throughput(logs, proposer_count, throughput)
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
