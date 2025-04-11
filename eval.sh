#!/usr/bin/env bash

CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(k-census e-paxos multi-paxos paxos weak-replication)
ALGOS=(no-replication ${REPLICATED_ALGOS[@]})
CONFIGS=(aws-europe-7-alt.toml aws-north-america-7.toml aws-world-ring-13.toml) # aws-europe-7.toml aws-world-ring-9.toml
YCSB=(1 0.5 0.05)
REQUESTS=100
SPEEDUP=1

function digits() {
  echo "$1" | tr -d -c 0-9
}

function start_cassandra() {
  local NB="$1"
  for i in $(seq 1 "$NB"); do
    (
      local name="cassandra-$i"
      if [ -z "$(docker ps -a -q --filter="name=$name")" ]; then
        docker run -e JVM_OPTS="-Xms256M -Xmx1024M" --name "$name" -p $((CASSANDRA_BASE_PORT + i - 1)):9042 -d cassandra
      else
        echo "$name already running" >/dev/null
      fi
    ) &
  done
  wait
  for i in $(seq 1 "$NB"); do
    (
      local name="cassandra-$i"
      until docker exec "$name" cqlsh -e "SELECT now() FROM system.local;" > /dev/null 2>&1; do
        echo "Waiting for $name to be ready..."
        sleep 2
      done
      echo "$name ready" >/dev/null
    ) &
  done
  wait
}

function stop_cassandra() {
  docker ps -a -q --filter="name=cassandra" | xargs docker rm -f
}

function run() {
  local CONFIG="$1"
  local ALGO="$2"
  local WRITES="$3"
  local REQUESTS="$4"
  local INGRESS="$5"
  local THROUGHPUT="$6"
  local FAULTS="$7"
  local TITLE="c=$CONFIG/a=$ALGO/w=$WRITES/r=$REQUESTS/i=$INGRESS/t=$THROUGHPUT/s=$SPEEDUP/f=$FAULTS"
  local LOG_DIR="$BASE_LOG_DIR/$TITLE/"
  mkdir -p "$LOG_DIR"
  killall kcensus 2>/dev/null
  local NB=$(digits "$CONFIG")
  if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
    start_cassandra "$NB"
  fi
  echo "Starting $TITLE"
  for pid in $(seq 0 $((NB - 1))); do
    local CASSANDRA_ARG=""
    if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
      CASSANDRA_ARG="-d 127.0.0.1:$((CASSANDRA_BASE_PORT + pid))"
    fi
    cargo run -r -- -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" -f "$FAULTS" >"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
  done
  wait
}

# No load, pure latency
function exp-1() {
  for writes in "${YCSB[@]}"; do
    for config in "${CONFIGS[@]}"; do
      for algo in "${ALGOS[@]}"; do
        run "$config" "$algo" "$writes" "$REQUESTS" round-robin 0
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 1-bars.py -c "$config" -w "$writes" -r "$REQUESTS" -i round-robin -t 0 -s "$SPEEDUP" &&
        python3 2-cdfs.py -c "$config" -w "$writes" -r "$REQUESTS" -i round-robin -t 0 -s "$SPEEDUP" &&
        cd ..
      )
    done
  done
}

# Latency under load
function exp-2() {
  local LOADS=(10 100 1000) # req/s per client
  for writes in "${YCSB[@]}"; do
    for config in "${CONFIGS[@]}"; do
      for load in "${LOADS[@]}"; do
        for algo in "${ALGOS[@]}"; do
          run "$config" "$algo" "$writes" "$REQUESTS" exponential "$load"
        done
      done
    done
  done
}

# Scalability
function exp-3() {
  local LARGE_CONFIGS=(aws-all-31.toml)
  for writes in "${YCSB[@]}"; do
    for config in "${LARGE_CONFIGS[@]}"; do
      for algo in "${ALGOS[@]}"; do
        run "$config" "$algo" "$writes" "$REQUESTS" round-robin 0
      done
    done
  done
}

function all_faults() {
  local REPLICAS="$1"
  local MAJORITY=$((REPLICAS / 2))
python3 - <<END
from itertools import combinations
for r in range(1, $MAJORITY + 1):
    for comb in combinations(range($REPLICAS), r):
        print(','.join(map(str, comb)), end=' ')
END
}

# Faults
function exp-4() {
  local config=aws-world-ring-9.toml
  local writes=1
  local requests=10
  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in $(all_faults 9); do
      run "$config" "$algo" $writes $requests round-robin 0 "$faults"
    done
  done
}

exp-1
exp-2
exp-3
exp-4