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
  local duration="10s"
  # Experiment 1 produces TWO figures, in two different sections of the paper:
  # Figure 1 (Introduction) and Figure 7 (End-to-End Latency). Both come from the same runs,
  # so they are always plotted together.
  (
    cd graphs &&
    echo -n 'plotting exp-1 figure 1/2 (Figure 1)... ' &&
    python3 exp-1-figure-1-intro.py $LOCAL_FLAG -d "$duration" -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-1-figure-1-intro.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-1 figure 2/2 (Figure 7)... ' &&
    python3 exp-1-figure-7-latency.py $LOCAL_FLAG -d "$duration" -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-1-figure-7-latency.txt" &&
    echo 'done.'
  )
}

function exp-2() {
  local config=aws-ring-7
  local writes=1
  local duration="10s"
  local baseline_duration="10s"
  local throughput=1000
  local keys=100000
  (
    cd graphs &&
    echo -n 'plotting exp-2 figure 1/1 (Figure 8)... ' &&
    python3 exp-2-figure-8-faults.py $LOCAL_FLAG -c "$config" -w "$writes" --duration "$duration" --baseline-duration "$baseline_duration" -i exponential -t $throughput -s "$SPEEDUP" --shards $keys --keys $keys -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-2-figure-8-faults.txt" &&
    echo 'done.'
  )
}

function exp-3() {
  # The aws-random half of experiment 3 also records the traffic, CPU and peak-memory values
  # needed by Figures 10 and 11.
  (
    cd graphs &&
    echo -n 'plotting exp-3 figure 1/4 (Figure 9)... ' &&
    python3 exp-3-figure-9-scalability.py $LOCAL_FLAG  -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-9-scalability.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-3 figure 2/4 (Figure 10)... ' &&
    python3 exp-3-figure-10-network.py $LOCAL_FLAG -c aws-random/@.toml -w 1 --duration 10s -i exponential -t 1000 -s 1 --shards 100000 --keys 100000 --skew 0 -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-10-network.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-3 figure 3/4 (Figure 11)... ' &&
    python3 exp-3-figure-11-cpu-mem.py $LOCAL_FLAG -c aws-random/@.toml -w 1 --duration 10s -i exponential -t 1000 -s 1 --shards 100000 --keys 100000 --skew 0 -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-11-cpu-mem.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-3 figure 4/4 (Figure 12)... ' &&
    python3 exp-3-figure-12-propagation.py $LOCAL_FLAG -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-12-propagation.txt" &&
    echo 'done.'
  )
}

function exp-4() {
  # Optional dedicated resource run. The normal `all` workflow builds these figures from
  # exp-3 instead; this command remains able to plot explicitly collected exp-4 logs.
  local writes=1.0
  local duration="10s"
  local throughput=1000
  local speedup=1
  local keys=100000
  local skew=0.0
  (
    cd graphs &&
    echo -n 'plotting exp-4 figure 1/2 (Figure 10)... ' &&
    python3 exp-3-figure-10-network.py $LOCAL_FLAG --log-subdir exp-4 -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $keys --keys $keys --skew $skew -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-10-network.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-4 figure 2/2 (Figure 11)... ' &&
    python3 exp-3-figure-11-cpu-mem.py $LOCAL_FLAG --log-subdir exp-4 -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $keys --keys $keys --skew $skew -g 1 > "./plots/${LOCAL_FLAG:+local-}exp-3-figure-11-cpu-mem.txt" &&
    echo 'done.'
  )
}

function exp-5() {
  # Contention and load.
  (
    cd graphs &&
    echo -n 'plotting exp-5 figure 1/2 (CDFs)... ' &&
    python3 exp-5-cdfs.py $LOCAL_FLAG ${1+"$@"} > "./plots/${LOCAL_FLAG:+local-}exp-5-cdfs.txt" &&
    echo 'done.' &&
    echo -n 'plotting exp-5 figure 2/2 (load)... ' &&
    python3 exp-5-load.py $LOCAL_FLAG ${1+"$@"} > "./plots/${LOCAL_FLAG:+local-}exp-5-load.txt" &&
    echo 'done.'
  )
}

function show_help() {
    cat << EOF
Usage: ./plot.sh [--local] [COMMAND]

Available commands:
  exp-1            Figures 1 and 7   - intro teaser and end-to-end latency
  exp-2            Figure  8         - impact of failures on latency
  exp-3            Figures 9-12      - scalability, resources, and optimization time
  exp-4            Figures 10 and 11 - optional dedicated resource-run logs
  exp-5            Figures 13 and 14 - contention and load (CDFs/conflicts, throughput)
  all              Plot every figure in the paper

The command names match ./eval.sh, so whatever you ran, plot it with the same name.
(plot-1 .. plot-5 are also accepted.)

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
  # Experiment 5 is AWS-only, so local `all` stops after Figure 12.
  if [ -z "$LOCAL_FLAG" ]; then exp-5; fi
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
    "exp-5")
      exp-5 "$@"
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
