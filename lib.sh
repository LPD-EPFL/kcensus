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

REPLICATED_ALGOS=(kcensus "swift-paxos" pando epaxos "multi-paxos" paxos)
ALGOS=("${REPLICATED_ALGOS[@]}")

DURATION=10s
THROUGHPUT=1000 # total req/s, split evenly between the proposers
SPEEDUP=1
KEYS=10000
SKEW=0
SHARDS=$KEYS

# Experiment 1 (Figures 1 and 7): four 7-replica deployments.
EXP1_CONFIGS=("aws-ring-7" "aws-europe-7" "aws-north-america-7" "aws-east-asia-7")
# Experiment 2 (Figure 8): faults, on the Northern-Hemisphere deployment.
EXP2_CONFIG="aws-ring-7"
# Experiment 3 (Figures 9 and 12): 3..31 replicas, two placement strategies.
EXP3_TYPES=(aws-from-paris aws-random)
EXP3_SIZES=(31 29 27 25 23 21 19 17 15 13 11 9 7 5 3)
# Experiment 4 (Figures 10 and 11): resource consumption, one machine, sped-up clock.
EXP4_TYPE=aws-random
EXP4_SIZES=(3 5 7 9 11 13 15 17 19 21 23 25 27 29 31)
EXP4_SPEEDUP=2

# Experiment 5 (no figure yet): contention and load, on the Northern-Hemisphere deployment.
EXP5_CONFIG="aws-ring-7"
EXP5_WRITES=0.5
# Zipf exponent over `EXP5_KEYS` keys. 0.99 is YCSB's default constant and the usual "skewed"
# point in the literature; 0 is uniform, and even there 10000 keys still conflict occasionally.
EXP5_SKEWS=(0 0.5 0.75 0.99)
EXP5_KEYS=10000
EXP5_KEYS_SPARSE=100000
# 500 to 16000 in exponential steps
EXP5_THROUGHPUTS=($(awk 'BEGIN { for (i = 0; i <= 50; i++) printf "%d ", int(500 * 2 ^ (i / 10) + 0.5) }'))
EXP5_CDF_THROUGHPUT=1000
EXP5_LOAD_SKEWS=(0.5 0.99)

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
  exp-3             Scalability + optimization time -> Figures 9 and 12
  exp-4             Resource consumption            -> Figures 10 and 11
  all               Run every experiment the paper depends on (exp-1..exp-4)
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
#   1           -- all of them. Experiment 4 needs this: it measures compute, which is essentially
#                  proportional to the number of requests processed. A shorter local run would
#                  understate CPU, and understate it *more* at larger n -- distorting the very
#                  axis Figure 11 plots against.
#
# `local_duration` in graphs/common.py must compute the same value, or the `d=` in the path it
# looks for will not exist.
function local_duration() {
  local num_replicas="$1" sample_divisor="${2:-2}"
  local base="${DURATION%s}"
  local target=$(( (THROUGHPUT * base) / sample_divisor ))
  local rate; rate="$(local_throughput "$num_replicas")"
  awk -v target="$target" -v rate="$rate" -v base="$base" \
      'BEGIN { d = target / rate; d = (d == int(d) ? d : int(d) + 1); print (d > base ? d : base) }'
}

# Every fault combination up to a minority of the voting replicas.
function all_faults() {
python3 - <<END
from itertools import combinations
SERVERS=$1
NON_VOTERS=[$2]
FROM="$3"
TO="$4"
VOTERS=list(range(SERVERS))
for non_voter in NON_VOTERS:
  VOTERS.remove(non_voter)
MINORITY=(len(VOTERS) - 1) // 2
done = False
should_yield = FROM == ''
for r in range(1, MINORITY + 1):
    if done: break
    for comb in combinations(VOTERS, r):
        formatted = ','.join(map(str, comb))
        if formatted == FROM: should_yield = True
        if formatted == TO and TO != '': done = True; break;
        if should_yield:
          print(formatted, end=' ')
END
}

# Replicas that take part in the protocol but do not vote. Only the older, non-2f+1 deployments
# need this; the four 7-replica deployments the paper uses have every replica voting.
get_nonvoting() {
  local config=$1
  local algo=$2
  case "$config-$algo" in
    aws-europe-8-weak-replication) echo "4";;
    aws-europe-8-kcensus) echo "4";;
    aws-europe-8-swift-paxos) echo "4";;
    aws-europe-8-pando) echo "4";;
    aws-europe-8-epaxos) echo "4";;
    aws-europe-8-multi-paxos) echo "4";;
    aws-europe-8-multi-paxos-3p) echo "4";;
    aws-europe-8-paxos) echo "4";;

    aws-east-asia-9-weak-replication) echo "1,8";;
    aws-east-asia-9-kcensus) echo "1,8";;
    aws-east-asia-9-swift-paxos) echo "2,4";;
    aws-east-asia-9-pando) echo "2,4";;
    aws-east-asia-9-epaxos) echo "1,4";;
    aws-east-asia-9-multi-paxos) echo "2,4";; # any pair composed of 0,1,2,3,4 works
    aws-east-asia-9-multi-paxos-3p) echo "2,4";;
    aws-east-asia-9-paxos) echo "1,8";;

    *) echo "";;
  esac
}
