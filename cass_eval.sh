#!/usr/bin/env bash

CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(k-census e-paxos multi-paxos paxos weak-replication)
ALGOS=(no-replication ${REPLICATED_ALGOS[@]})
REQUESTS=50000
SPEEDUP=1
KEYS=20000

if ! command -v "/usr/bin/time" >/dev/null 2>&1
then
    echo "/usr/bin/time not installed"
    exit 1
fi

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
  local KEYS="$7"
  local FAULTS="$8"
  local TITLE="c=$CONFIG/a=$ALGO/w=$WRITES/r=$REQUESTS/i=$INGRESS/t=$THROUGHPUT/s=$SPEEDUP/f=$FAULTS/k=$KEYS"
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
    local FAULTS_ARG=""
    if [[ "$FAULTS" != "" ]]; then
       FAULTS_ARG="-f $FAULTS"
    fi
    cargo build -r 2>"$LOG_DIR/$pid.stderr"
    local time_format='[log=time] Memory (KB): %M, System (s): %S User (s): %U | {"memory": %M, "system": %S, "user": %U}'
    (/usr/bin/time -f "$time_format" target/release/kcensus -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG -k "$KEYS")>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
  done
  wait
}

function main() {
  for writes in "1"; do
    for config in "localhost-3.toml"; do
      for algo in "k-census"; do
        run "$config" "$algo" "$writes" "$REQUESTS" exponential 4000 "$KEYS"
      done
    done
  done
}

main