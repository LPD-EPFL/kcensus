#!/usr/bin/env bash

# Abort as soon as a plot fails, so a broken figure cannot be mistaken for a clean run.
set -e

SPEEDUP=1

# `--local` plots the output of `eval.sh --local` (./local-logs) instead of the AWS results
# (./logs). It is forwarded to every figure script, which also applies the reduced local
# throughput and stretched window when rebuilding log paths -- see local_throughput and
# local_duration in lib.sh and graphs/common.py. It may appear anywhere in the arguments.
LOCAL_FLAG=""
ARGS=()
for arg in "$@"; do
  if [ "$arg" = "--local" ]; then LOCAL_FLAG="--local"; else ARGS+=("$arg"); fi
done
set -- ${ARGS[@]+"${ARGS[@]}"}

function parse_geo_logs() {
  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 parse_geo_logs.py
  )
}

function activate_env() {
  pushd graphs >/dev/null
  source env.sh >/dev/null 2>&1
  popd >/dev/null
}

function exp-1() {
  # Experiment 1 produces TWO figures, in two different sections of the paper:
  # Figure 1 (Introduction) and Figure 7 (End-to-End Latency). Both come from the same runs,
  # so they are always plotted together.
  (
    cd graphs &&
    python3 exp-1-figure-1-intro.py $LOCAL_FLAG   -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-1-figure-1-intro.txt" &&
    echo 'finished figure 1/2 (Figure 1).' &&
    python3 exp-1-figure-7-latency.py $LOCAL_FLAG -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-1-figure-7-latency.txt" &&
    echo 'finished figure 2/2 (Figure 7).'
  )
}

function exp-2() {
  local config=aws-ring-7
  local writes=1
  local duration="10s"
  local throughput=1000
  local shards=10000
  local keys=10000
  (
    cd graphs &&
    python3 exp-2-figure-8-faults.py $LOCAL_FLAG -c "$config" -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$SPEEDUP" --shards $shards --keys $keys -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-2-figure-8-faults.txt" &&
    echo 'finished figure (Figure 8).'
  )
}

function exp-3() {
  # Experiment 3 also produces two figures: Figure 9 (Scalability) and Figure 12 (Time to
  # Optimize Requirements). Both come from the same 31-node deployment.
  (
    cd graphs &&
    python3 exp-3-figure-9-scalability.py $LOCAL_FLAG  -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-9-scalability.txt" &&
    echo 'finished figure 1/2 (Figure 9).' &&
    python3 exp-3-figure-12-propagation.py $LOCAL_FLAG -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-12-propagation.txt" &&
    echo 'finished figure 2/2 (Figure 12).'
  )
}

function exp-4() {
  # Figures 10 (traffic/messages) and 11 (CPU/memory), both in Resource Consumption.
  local writes=1.0
  local duration="10s"
  local throughput=1000
  local speedup=2
  local keys=10000
  local skew=0.0
  local shards=10000
  (
    cd graphs &&
    python3 exp-4-figure-10-network.py $LOCAL_FLAG -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $shards --keys $keys --skew $skew -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-4-figure-10-network.txt" &&
    echo 'finished figure 1/2 (Figure 10).' &&
    python3 exp-4-figure-11-cpu-mem.py $LOCAL_FLAG -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $shards --keys $keys --skew $skew -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-4-figure-11-cpu-mem.txt" &&
    echo 'finished figure 2/2 (Figure 11).'
  )
}

# Legacy: the conflicts/CDF plot maps to no figure in the current paper and needs exp-conflicts
# data, which is not produced by default. Kept as the basis for the camera-ready experiment.
function exp-conflicts() {
  (
    cd graphs &&
    python3 exp-conflicts-cdfs.py $LOCAL_FLAG -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-conflicts-cdfs.txt" &&
    echo 'finished plot.'
  )
}

function show_help() {
    cat << EOF
Usage: ./plot.sh [--local] [COMMAND]

Available commands:
  exp-1            Figures 1 and 7   - intro teaser and end-to-end latency
  exp-2            Figure  8         - impact of failures on latency
  exp-3            Figures 9 and 12  - scalability, and time to optimize requirements
  exp-4            Figures 10 and 11 - resource consumption (traffic/messages, CPU/memory)
  all              Plot every figure in the paper

The command names match ./eval.sh, so whatever you ran, plot it with the same name.
(plot-1 .. plot-4 are also accepted.)

Options:
  --local          Plot ./local-logs (produced by eval.sh) instead of ./logs. Figures are
                   written with a 'local-' prefix so they never overwrite the real ones.
  help/-h/--help   Show help

EOF
}

function run_all_plots() {
  exp-1
  exp-2
  exp-3
  exp-4
  # exp-conflicts is not included: it maps to no figure in the current paper and needs
  # exp-conflicts data, which `geo_eval.sh all` does not produce.
}

function main() {
  if [[ $# -eq 0 ]]; then
    show_help
    exit 0
  fi

  activate_env
  parse_geo_logs

  local command="$1"
  shift

  # Commands are named exp-N to match ./eval.sh -- each draws exactly the figures that
  # experiment produces. plot-N is the old spelling, still accepted.
  command="${command/#plot-/exp-}"

  case "$command" in
    "exp-1")
      exp-1
      ;;
    "exp-2")
      exp-2
      ;;
    "exp-3")
      exp-3
      ;;
    "exp-4")
      exp-4
      ;;
    "exp-conflicts")
      exp-conflicts
      ;;
    "all")
      run_all_plots
      ;;
    "help"|"-h"|"--help")
      show_help
      ;;
    *)
      echo "Error: Unknown command '$command'"
      echo ""
      show_help
      exit 1
      ;;
  esac
}

main "$@"
