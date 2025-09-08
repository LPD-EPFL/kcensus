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

if [[ "$1" == "y" ]]; then
  CASSANDRA="true"
elif [[ "$1" == "n" ]]; then
  CASSANDRA="false"
else
  echo "Usage: $0 [y|n]"
  echo "  y: run with Cassandra"
  echo "  n: run without Cassandra"
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
        docker run \
          -e JVM_OPTS="-Xms512M -Xmx1G -XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0" \
          -e CASSANDRA_MEMTABLE_ALLOCATION_TYPE="heap_buffers" \
          -e CASSANDRA_MEMTABLE_CLEANUP_THRESHOLD="0.2" \
          -e CASSANDRA_CONCURRENT_WRITES="64" \
          -e CASSANDRA_CONCURRENT_READS="64" \
          -e CASSANDRA_FILE_CACHE_SIZE_IN_MB="0" \
          -e CASSANDRA_BUFFER_POOL_USE_HEAP_IF_EXHAUSTED="true" \
          -e CASSANDRA_DISK_OPTIMIZATION_STRATEGY="ssd" \
          -e CASSANDRA_COMMITLOG_SYNC="batch" \
          -e CASSANDRA_COMMITLOG_SYNC_BATCH_WINDOW_IN_MS="1" \
          -e CASSANDRA_COMMITLOG_SEGMENT_SIZE_IN_MB="32" \
          -e CASSANDRA_AUTO_SNAPSHOT="false" \
          -e CASSANDRA_SNAPSHOT_BEFORE_COMPACTION="false" \
          --tmpfs /var/lib/cassandra:noexec,nosuid,size=1500m \
          --tmpfs /var/log/cassandra:noexec,nosuid,size=100m \
          --memory=2g \
          --cpus="1" \
          --name "$name" \
          -p $((CASSANDRA_BASE_PORT + i - 1)):9042 \
          -d cassandra
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
    if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
      (/usr/bin/time -f "$time_format" target/release/kcensus -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG --simulate-delays false -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG -k "$KEYS")>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
    else
      (/usr/bin/time -f "$time_format" target/release/kcensus -p "$pid" --config "configs/$CONFIG" --simulate-delays false -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG -k "$KEYS")>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
    fi
  done
  wait
  head -n 5 "$LOG_DIR/0.stdout"
  tail -n 5 "$LOG_DIR/0.stdout"
}

function main() {
  for writes in "1"; do
    for config in "aws-europe-3.toml"; do
      for algo in "k-census"; do
        run "$config" "$algo" "$writes" "$REQUESTS" exponential 10000 "$KEYS"
      done
    done
  done
}

main