#!/usr/bin/env bash
#
# Definitions shared by geo_eval.sh (AWS) and eval.sh (local).
#
# Everything that describes *what* an experiment is lives here, so the two runners cannot drift
# apart. Drift is what made the previous local runner unusable: it had fallen behind on configs
# and on the log-path schema, and its output could no longer be plotted at all.
#
# Not shared: how a run is executed (Ansible on EC2 vs local processes) and the AWS-only
# provisioning, retry-backoff and teardown machinery.

# This order is intentionnal: it makes sure paxos (unplotted) serves as warmup for the switch caches,
# and keeps the most important baselines (epaxos and swiftpaxos) close to kcensus in run order.
REPLICATED_ALGOS=(paxos "multi-paxos" pando epaxos kcensus "swift-paxos")
ALGOS=("${REPLICATED_ALGOS[@]}")

DURATION=10s
# Longer runs for plots that show percentiles. (exp-1, exp-2 0-faults, exp-5 CDF)
BASELINE_DURATION=10s
THROUGHPUT=1000 # total req/s, split evenly between the proposers
SPEEDUP=1
KEYS=100000
SKEW=0
SHARDS=$KEYS

# Experiment 1 (Figures 1 and 7): four 7-replica deployments.
EXP1_CONFIGS=("aws-ring-7" "aws-europe-7" "aws-north-america-7" "aws-east-asia-7")
# Experiment 2 (Figure 8): faults, on the Northern-Hemisphere deployment.
EXP2_CONFIG="aws-ring-7"
# Experiment 3 (Figures 9-12): 3..31 replicas, two placement strategies, to measure scalability and resource-consumption.
EXP3_TYPES=(aws-from-paris aws-random)
EXP3_SIZES=(31 29 27 25 23 21 19 17 15 13 11 9 7 5 3)
# Experiment 4 (Figures 10 and 11): resource consumption on one machine, in real time.
EXP4_TYPE=aws-random
EXP4_SIZES=(3 5 7 9 11 13 15 17 19 21 23 25 27 29 31)

# Experiment 5 (Figures 13 and 14): contention and load, on the Northern-Hemisphere deployment.
EXP5_CONFIG="aws-ring-7"
EXP5_WRITES=0.5
# Zipf exponent. 0.99 is YCSB's default constant and the usual "skewed"
# point in the literature; 0 is uniform.
EXP5_SKEWS=() # No Skews, since EXP5_LOAD_SKEWS lists those and BASELINE_DURATION == DURATION
# The load ladder doubles until a rung cannot be sustained, then refines around the last one
# that could. Walked per algorithm and per skew: they meet their walls decades apart, so a
# shared ladder spends most of its runs on rates one algorithm cannot reach and another
# passed long ago.
EXP5_LADDER_START=500 # Just to have a point bellow 1000
EXP5_LADDER_END=1024000 # Way too large, just in case (likely to stop before 64k anyway)
# One refinement step: a quarter of the last sustained rung, capped at this many req/s.
EXP5_REFINE_STEP_CAP=4000
# Series that sustained less than this skip the two half-step probes below their wall.
EXP5_PROBE_MIN_SUSTAINED=4000
EXP5_CDF_THROUGHPUT=1000
EXP5_LOAD_SKEWS=(0 0.99)

# The load ladder, shared by both runners so the rungs cannot drift apart.
#
# Each runner defines `exp-5-series-run <algo|skew|conflicts> <throughput>`, returning non-zero
# when the rate was not sustained. Everything else -- which rungs are walked, how a failure is
# retried, and when a series is dropped -- lives here.

# The series the load figure plots: every algorithm at every load skew, plus multi-paxos once
# more with pipelining. One `algo|skew|conflicts` entry per line of the figure.
function exp-5-series() {
  local skew algo
  for skew in "${EXP5_LOAD_SKEWS[@]}"; do
    for algo in "${ALGOS[@]}"; do
      echo "${algo}|${skew}|true"
    done
  done
  echo "multi-paxos|0|false"
}

# Walks the load ladder for every series at once.
#
# Runs are ordered by offered rate rather than by series: the figure compares algorithms at a
# rate, and a sweep this long outlives the network conditions it started in, so two series
# measured at one rate are measured minutes apart rather than hours.
#
# Phase 1 doubles. Doubling finds the wall in a logarithmic number of runs wherever it is,
# which is what lets one ladder definition serve algorithms whose walls are two decades apart.
#
# A rate that fails is retried once, at the same rate, in the next round -- a rate lost to a
# slow instance or a retried deployment would otherwise anchor the refinement a full factor of
# two below the real wall, and the retry lands after the rest of the deployment has moved on.
# The retry comes first in that round, so the series is back at the shared rate before the
# round ends and stays comparable with the others. Failing the same rate twice ends the series.
function exp-5-ladder() {
  local active=("$@") survivors=()
  local -A sustained=() pending=()
  local entry throughput="$EXP5_LADDER_START"

  while [ "$throughput" -le "$EXP5_LADDER_END" ] && [ "${#active[@]}" -gt 0 ]; do
    survivors=()
    for entry in "${active[@]}"; do
      if [ -n "${pending[$entry]:-}" ]; then
        if ! exp-5-series-run "$entry" "${pending[$entry]}"; then
          continue
        fi
        sustained["$entry"]="${pending[$entry]}"
        unset "pending[$entry]"
      fi
      if exp-5-series-run "$entry" "$throughput"; then
        sustained["$entry"]="$throughput"
      else
        pending["$entry"]="$throughput"
      fi
      survivors+=("$entry")
    done
    active=("${survivors[@]}")
    throughput=$((throughput * 2))
  done

  # Phase 2, one group per distinct last-sustained rate, so the series refined together are
  # the ones whose rungs coincide.
  local -A groups=()
  for entry in "${!sustained[@]}"; do
    groups["${sustained[$entry]}"]+="${entry} "
  done
  local rate
  for rate in $(printf '%s\n' "${!groups[@]}" | sort -n); do
    exp-5-refine "$rate" ${groups[$rate]}
  done
}

# Walks from three quarters of the rate a group of series last sustained up to twice it, which
# is where phase 1's doubling puts the wall.
#
# The rungs are one step apart -- a quarter of the sustained rate, capped at
# `EXP5_REFINE_STEP_CAP` -- so the spacing stays bounded as the rates grow instead of widening
# with them. The sustained rate itself is skipped, phase 1 having already measured it, and the
# walk stops short of twice it, which phase 1 found unsustainable.
#
# Failures are handled as in phase 1: a rate that fails is retried once at the same rate, at
# the start of the next round, so the series is back on the shared rate before that round ends
# and the figure keeps comparing algorithms measured minutes apart. Failing one rate twice
# ends the series -- one failure is as likely to be a slow instance as a real wall.
#
# That second failure is the series' wall, and it is then measured half a step and three halves
# of a step below it, placing two points inside the step the wall falls in. Groups that
# sustained less than `EXP5_PROBE_MIN_SUSTAINED` skip those two runs.
function exp-5-refine() {
  local sustained="$1"; shift
  local active=("$@") survivors=()
  local -A pending=() wall=() probes=()
  local rung entry step half first
  local -a rungs=()

  step="$(awk -v s="$sustained" -v cap="$EXP5_REFINE_STEP_CAP" \
    'BEGIN { q = int(s / 4); printf "%d", (q < cap ? q : cap) }')"
  [ "${step}" -gt 0 ] || step=1

  first="$(awk -v s="$sustained" 'BEGIN { printf "%d", s * 0.75 + 0.5 }')"
  for ((rung = first; rung < 4 * sustained; rung += step)); do
    if [ "${rung}" -ne "${sustained}" ]; then
      rungs+=("${rung}")
    fi
  done

  echo "--> refining around ${sustained} req/s in steps of ${step}: ${active[*]}"
  for rung in "${rungs[@]}"; do
    if [ "${#active[@]}" -eq 0 ]; then
      break
    fi
    survivors=()
    for entry in "${active[@]}"; do
      if [ -n "${pending[$entry]:-}" ]; then
        if ! exp-5-series-run "$entry" "${pending[$entry]}"; then
          wall["$entry"]="${pending[$entry]}"
          continue
        fi
        unset "pending[$entry]"
      fi
      if ! exp-5-series-run "$entry" "$rung"; then
        pending["$entry"]="$rung"
      fi
      survivors+=("$entry")
    done
    active=("${survivors[@]}")
  done

  if [ "${sustained}" -lt "${EXP5_PROBE_MIN_SUSTAINED}" ]; then
    return 0
  fi

  # Half-step probes, rate-major like every other round.
  for entry in "${!wall[@]}"; do
    for half_steps in 3 1; do
      rung=$(( ${wall[$entry]} - half_steps * step / 2 ))
      if [ "${rung}" -gt 0 ]; then
        probes["$rung"]+="${entry} "
      fi
    done
  done
  for rung in $(printf '%s\n' "${!probes[@]}" | sort -n); do
    for entry in ${probes[$rung]}; do
      exp-5-series-run "$entry" "$rung" || true
    done
  done
}

# A failed run is rare, so exhausting the attempts means something is genuinely wrong.
MAX_ATTEMPTS=5

# The one help text. eval.sh, geo_eval.sh and local_eval.sh all print this, so the command list
# cannot drift between them.
function show_help() {
    cat << HELP
Usage: ./eval.sh [--local] [--run-id ID] [COMMAND]

Runs the KCensus experiments. Without --local they run on AWS, provisioning and tearing down a
deployment per experiment; with --local every replica runs as a process on this machine and the
wide-area link delays are simulated. The commands are the same either way, and --local may go
before or after the command.

For AWS runs, --run-id adds a unique suffix to every AWS resource name so multiple reviewers can
run concurrently in the same account. Use 1-32 lowercase letters, digits, or hyphens, for example:
  ./eval.sh --run-id reviewer-1 exp-2

Available commands:
  exp-1             End-to-end latency              -> Figures 1 and 7
  exp-2             Impact of failures              -> Figure 8
  exp-3             Scalability + resources         -> Figures 9, 10, 11 and 12
  exp-4             Deprecated resource run         -> None (Old Figures 10 and 11)
  exp-5             Contention + throughput         -> Figures 13 and 14
  all               Run every experiment the paper depends on (exp-1..exp-3 + exp-5)
  destroy           Tear down a deployment by hand (AWS only):
                      ./eval.sh destroy <terraform-var-file> <experiment-id>
  help/-h/--help    Show this help

Results go to ./logs, or ./local-logs with --local. Plot them with the matching flag:
  ./plot.sh         exp-N
  ./plot.sh --local exp-N
HELP
}

function digits() {
  echo "$1" | tr -d -c 0-9
}

# The log path every run writes to, relative to the log root.
#
# Takes no arguments on purpose: it reads the caller's variables directly. Twelve positional
# parameters would be worse than the duplication this avoids -- one transposed pair and every
# path is silently wrong. Callers already name their locals this way; the loop below fails loudly
# if one is missing rather than emitting an empty path component.
#
# graphs/logparser.py rebuilds this exact string to read the logs back, so the two must agree.
function make_title() {
  local unset_vars=() v
  for v in configName algo writes duration ingress throughput speedup faults keys skew shards; do
    [ -z "${!v+set}" ] && unset_vars+=("$v")
  done
  if [ ${#unset_vars[@]} -gt 0 ]; then
    echo "make_title: caller left these unset: ${unset_vars[*]}" >&2
    return 1
  fi
  echo "c=${configName}/a=${algo}/w=${writes}/d=${duration}/i=${ingress}/t=${throughput}/s=${speedup}/f=${faults}/k=${keys}/skew=${skew}/shards=${shards}/conflicts=${conflicts:-false}"
}

# Local runs put every replica on one machine, so they cannot sustain the wide-area throughput.
# They are throttled by (f+1)/2 -- with f = (n-1)/2 that is a divisor of (n+1)/4, so 7 replicas
# run at half rate and 31 replicas at an eighth. Integer division, and `local_throughput` in
# graphs/common.py must compute exactly the same value or the plots will look in the wrong place.
function local_throughput() {
  local num_replicas="$1"
  echo $(( (THROUGHPUT * 4) / (num_replicas + 1) ))
}

# The rate each proposer is given, so that the deployment as a whole issues `total` req/s.
#
# Deliberately fractional: `-t` is an f32, and integer division here would silently lose the
# remainder. At 29 replicas `133/29` truncates to 4, i.e. 116 req/s against a path that claims
# 133 -- a 12.8% understatement, and one that varies with the replica count, which is exactly
# the axis the scalability figure plots against.
function per_proposer_rate() {
  awk -v t="$1" -v n="$2" 'BEGIN { printf "%.6g", t / n }'
}

# How long a local run measures for, in seconds.
#
# Local throughput falls with the replica count, so a fixed 10s window collects fewer and fewer
# requests as the deployment grows. Stretch the window to compensate.
#
# <sample_divisor> says how much of an AWS run's request count to reproduce:
#   2 (default) -- half. Enough for stable latency percentiles, and a local run is far less noisy
#                  than a wide-area one, so matching exactly would cost hours for no benefit.
#   1           -- all of them. Figure 11 needs this: it measures compute, which is essentially
#                  proportional to the number of requests processed. A shorter local run would
#                  understate CPU, and understate it *more* at larger n -- distorting the very
#                  axis Figure 11 plots against.
#
# `local_duration` in graphs/common.py must compute the same value, or the `d=` in the path it
# looks for will not exist.
function local_duration() {
  local num_replicas="$1" sample_divisor="${2:-2}" base="${3:-${DURATION%s}}"
  local target=$(( (THROUGHPUT * base) / sample_divisor ))
  local rate; rate="$(local_throughput "$num_replicas")"
  awk -v target="$target" -v rate="$rate" -v base="$base" \
      'BEGIN { d = target / rate; d = (d == int(d) ? d : int(d) + 1); print (d > base ? d : base) }'
}

# Every fault combination up to a minority of the replicas.
function all_faults() {
python3 - <<END
from itertools import combinations
SERVERS=$1
MINORITY=(SERVERS - 1) // 2
for r in range(1, MINORITY + 1):
    for comb in combinations(range(SERVERS), r):
        formatted = ','.join(map(str, comb))
        print(formatted, end=' ')
END
}
