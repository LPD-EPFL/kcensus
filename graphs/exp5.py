"""The exp-5 grid and log access, shared so the two plot scripts cannot disagree on it."""
import pathlib
import sys
from dataclasses import dataclass

import logparser
from common import ALGORITHMS, args, blue
from logparser import duration_to_ms, parse

EXP5_CONFIG = "aws-ring-7"
EXP5_DURATION = "10s"
EXP5_INGRESS = "exponential"
EXP5_ALGORITHMS = {
    **ALGORITHMS,
    "paxos": {
        "label": "Paxos",
        "color": blue,
        "lw": 0.7,
        "linestyle": "-",
        "marker": "v",
        "markersize": 2.2,
        "markeredgewidth": None,
    },
}

WRITES = 0.5
KEYS = 10000
# Ten times the conflict domain for the same distribution.
KEYS_SPARSE = 100000
CDF_THROUGHPUT = 1000
SKEWS = (0.0, 0.5, 0.75, 0.99)
# The only skews given the full ladder.
LOAD_SKEWS = (0.5, 0.99)


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
        return f"Zipf {self.skew:g}, {self.keys // 1000}k keys"

    @property
    def latency_xlim(self) -> float:
        """Where to cut the CDF, per skew."""
        return {0.0: 340, 0.5: 390, 0.75: 440, 0.99: 490}[self.skew]


def _rungs_on_disk():
    """Every `t=` with a run on disk, ascending.

    Read from the logs rather than mirrored from `EXP5_THROUGHPUTS`, so trimming the ladder
    in lib.sh needs no change here. Rungs that were never run are simply absent, which is
    what `run_stats` already reports as missing.
    """
    root = pathlib.Path(logparser.LOG_DIR) / f"c={EXP5_CONFIG}"
    pattern = f"a=*/w={WRITES:g}/d={EXP5_DURATION}/i={EXP5_INGRESS}/t=*"
    found = set()
    for path in root.glob(pattern):
        try:
            value = float(path.name.removeprefix("t="))
        except ValueError:
            continue
        found.add(int(value) if value.is_integer() else value)
    return tuple(sorted(found))


THROUGHPUTS = _rungs_on_disk()

CDF_WORKLOADS = tuple(
    Workload(skew, keys) for keys in (KEYS, KEYS_SPARSE) for skew in SKEWS
)
LOAD_WORKLOADS = tuple(
    Workload(skew, keys) for keys in (KEYS, KEYS_SPARSE) for skew in LOAD_SKEWS
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


def run_stats(algo, workload, throughput):
    """`(write_latencies_ms, achieved_req_per_s)` for one run, or `None` if it is not there.

    Missing is a result, not an error: the ladder runs past what the deployment sustains. Both
    figures come from one parse -- the top rungs run to hundreds of thousands of lines. Reads
    are left out of the latencies, being served from a read quorum rather than ordered.
    """
    try:
        logs = parse(
            config=EXP5_CONFIG,
            algo=algo,
            writes=WRITES,
            duration=EXP5_DURATION,
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
    # Counted, not read from `[log=throughput]`: that event only exists on the Cassandra path.
    completed = sum(len(items) for items in logs["executed"].values())
    achieved = completed / float(EXP5_DURATION.rstrip("s"))
    return latencies, achieved
