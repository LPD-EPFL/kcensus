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

activate_env


parse_geo_logs
plot-1
plot-2
plot-3
plot-4
plot-5
plot-6