#!/usr/bin/env bash

REQUESTS=100
SPEEDUP=1

if ! command -v "/usr/bin/time" >/dev/null 2>&1
then
    echo "/usr/bin/time not installed"
    exit 1
fi

function digits() {
  echo "$1" | tr -d -c 0-9
}

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
  (
    cd graphs &&
    python3 1-bars-merged.py -g 1 > "./plots/1-pure-latency.txt"
  )
}

function plot-2() {
  (
    cd graphs &&
    python3 2-cdfs-merged.py -g 1 > "./plots/2-load-latency.txt"
  )
}

function plot-3() {
  local requests=10
  (
    cd graphs &&
    python3 3-scalability-merged.py -g 1 > "./plots/3-scalability.txt"
  )
}

function plot-4() {
  local config=aws-world-ring-9
  local writes=1
  local requests=10
  (
    cd graphs &&
    python3 4-faults.py -c "$config" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP" -g 1 > "./plots/4-faults.txt"
  )
}

function plot-5() {
  (
    cd graphs &&
    python3 5-propagation.py -g 1 > "./plots/5-propagation.txt"
  )
}

function plot-6() {
  SPEEDUP=10000000
  local requests=1000
  (
    cd graphs &&
    python3 6-network.py -r 1000 -c aws-random/@.toml -s 10000000 -w 1 -t=0 -g 1 > "./plots/6-network.txt" &&
    python3 7-cpu-mem.py -r 1000 -c aws-random/@.toml -s 10000000 -w 1 -t=0 -g 1 > "./plots/7-cpu-mem.txt"
  )
}

function show_help() {
    cat << EOF
Usage: $0 [COMMAND]

Available commands:
  plot-1           Plot Experiment 1: Pure Latency
  plot-2           Plot Experiment 2: Latency under load
  plot-3           Plot Experiment 3: Scalability
  plot-4           Plot Experiment 4: Faults
  plot-5           Plot Experiment 5: Propagation
  plot-6           Plot Experiment 6: Resources
  all              Run all plots
  help/-h/--help   Show help

EOF
}

function run_all_plots() {
  plot-1
  plot-2
  plot-3
  plot-4
  plot-5
  plot-6
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
    "plot-5")
      plot-5
      ;;
    "plot-6")
      plot-6
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