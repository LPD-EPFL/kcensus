#!/usr/bin/env python3
"""Mean latency in milliseconds over every request a run measured, printed to stdout.

Puts and reads are pooled the way Figure 14 pools them, so the load ladder reads a rung the
same way the figure will.
"""
import json
import sys
from pathlib import Path

from check_expected_latency import EXECUTED_PREFIX, discover_process_logs
from logparser import duration_to_ms


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: run_average_latency.py <run-directory>", file=sys.stderr)
        return 2
    run_dir = Path(sys.argv[1]).expanduser()
    if not run_dir.is_dir():
        print(f"not a directory: {run_dir}", file=sys.stderr)
        return 2

    total = 0.0
    count = 0
    for process_logs in discover_process_logs(run_dir).values():
        for path in process_logs.values():
            with path.open(encoding="utf-8", errors="replace") as log:
                for line in log:
                    if not line.startswith(EXECUTED_PREFIX):
                        continue
                    try:
                        payload = json.loads(line.split(" | ", maxsplit=1)[1])
                        total += duration_to_ms(payload["latency"])
                        count += 1
                    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
                        pass

    if not count:
        print(f"no measured requests in {run_dir}", file=sys.stderr)
        return 1
    print(f"{total / count:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
