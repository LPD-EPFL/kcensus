#!/usr/bin/env bash

# Abort as soon as a plot fails, so a broken figure cannot be mistaken for a clean run.
set -e

SPEEDUP=1

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

function plot-1() {
  # Experiment 1 produces TWO figures, in two different sections of the paper:
  # Figure 1 (Introduction) and Figure 7 (End-to-End Latency). Both come from the same runs,
  # so they are always plotted together.
  (
    cd graphs &&
    python3 exp-1-figure-1-intro.py   -g 1 > "./plots/exp-1-figure-1-intro.txt" &&
    echo 'finished figure 1/2 (Figure 1).' &&
    python3 exp-1-figure-7-latency.py -g 1 > "./plots/exp-1-figure-7-latency.txt" &&
    echo 'finished figure 2/2 (Figure 7).'
  )
}

function plot-2() {
  local config=aws-ring-7
  local writes=1
  local duration="10s"
  local throughput=1000
  local shards=10000
  local keys=10000
  (
    cd graphs &&
    python3 exp-2-figure-8-faults.py -c "$config" -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$SPEEDUP" --shards $shards --keys $keys -g 1 > "./plots/exp-2-figure-8-faults.txt" &&
    echo 'finished figure (Figure 8).'
  )
}

function plot-3() {
  # Experiment 3 also produces two figures: Figure 9 (Scalability) and Figure 12 (Time to
  # Optimize Requirements). Both come from the same 31-node deployment.
  (
    cd graphs &&
    python3 exp-3-figure-9-scalability.py  -g 1 > "./plots/exp-3-figure-9-scalability.txt" &&
    echo 'finished figure 1/2 (Figure 9).' &&
    python3 exp-3-figure-12-propagation.py -g 1 > "./plots/exp-3-figure-12-propagation.txt" &&
    echo 'finished figure 2/2 (Figure 12).'
  )
}

function plot-4() {
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
    python3 exp-4-figure-10-network.py -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $shards --keys $keys --skew $skew -g 1 > "./plots/exp-4-figure-10-network.txt" &&
    echo 'finished figure 1/2 (Figure 10).' &&
    python3 exp-4-figure-11-cpu-mem.py -c aws-random/@.toml -w "$writes" --duration "$duration" -i exponential -t $throughput -s "$speedup" --shards $shards --keys $keys --skew $skew -g 1 > "./plots/exp-4-figure-11-cpu-mem.txt" &&
    echo 'finished figure 2/2 (Figure 11).'
  )
}

# Legacy: the conflicts/CDF plot maps to no figure in the current paper and needs exp-conflicts
# data, which is not produced by default. Kept as the basis for the camera-ready experiment.
function plot-conflicts() {
  (
    cd graphs &&
    python3 exp-conflicts-cdfs.py -g 1 > "./plots/exp-conflicts-cdfs.txt" &&
    echo 'finished plot.'
  )
}

function show_help() {
    cat << EOF
Usage: $0 [COMMAND]

Available commands:
  plot-1           Figures 1 and 7  - intro teaser and end-to-end latency
  plot-2           Figure  8        - impact of failures on latency
  plot-3           Figures 9 and 12 - scalability, and time to optimize requirements
  plot-4           Figures 10 and 11 - resource consumption (traffic/messages, CPU/memory)
  all              Plot every figure in the paper
  plot-conflicts   Legacy CDF plot - maps to no figure in the current paper
  help/-h/--help   Show help

EOF
}

function run_all_plots() {
  plot-1
  plot-2
  plot-3
  plot-4
  # plot-conflicts is not included: it maps to no figure in the current paper and needs
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

  case "$command" in
    "plot-1")
      plot-1
      ;;
    "plot-2")
      plot-2
      ;;
    "plot-3")
      plot-3
      ;;
    "plot-4")
      plot-4
      ;;
    "plot-conflicts")
      plot-conflicts
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
