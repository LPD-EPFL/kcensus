#!/usr/bin/env bash


CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
# REPLICATED_ALGOS=(kcensus "weak-replication" "swift-paxos" pando epaxos "multi-paxos" paxos)
# ALGOS=(no-replication "${REPLICATED_ALGOS[@]}")
REPLICATED_ALGOS=(kcensus "swift-paxos" pando epaxos "multi-paxos" paxos)
ALGOS=("${REPLICATED_ALGOS[@]}")
WRITES=(1)
SPEEDUP=2 # latency precision does not matter

# Bounded retries for a failed run: abort if 5 consecutive attempts fail. No delay is needed
# here -- everything runs locally on a single machine and `run` already does `killall kcensus`
# at the start of every attempt.
MAX_ATTEMPTS=5

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
        docker run -e JVM_OPTS="-Xms256M -Xmx1024M" --name "$name" -p $((CASSANDRA_BASE_PORT + i - 1)):9042 -d shotover/cassandra-test:5.0-rc1-r3
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

function run() {
  local CONFIG="$1"
  local ALGO="$2"
  local WRITES="$3"
  local DURATION="$4"
  local timeout_s=$(( 30 + 3 * ${DURATION%s} ))
  local INGRESS="$5"
  local THROUGHPUT="$6"
  local FAULTS="$7"
  local KEYS="$8"
  local SKEW="$9"
  local SHARDS="${10}"
  local TITLE="c=$CONFIG/a=$ALGO/w=$WRITES/d=$DURATION/i=$INGRESS/t=$THROUGHPUT/s=$SPEEDUP/f=$FAULTS/k=$KEYS/skew=$SKEW/shards=$SHARDS/conflicts=false"
  local LOG_DIR="$BASE_LOG_DIR/$TITLE/"
  local NB=$(digits "$CONFIG")
  local PER_PROPOSER_THROUGHPUT=$(($THROUGHPUT / $NB))
  mkdir -p "$LOG_DIR"
  killall kcensus 2>/dev/null
#  if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
#    start_cassandra "$NB"
#  fi
  echo "Starting $TITLE"
  pids=()
  for pid in $(seq 0 $((NB - 1))); do
    local CASSANDRA_ARG=""
#    if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
#      CASSANDRA_ARG="-d 127.0.0.1:$((CASSANDRA_BASE_PORT + pid))"
#    fi
    local FAULTS_ARG=""
    if [[ "$FAULTS" != "" ]]; then
       FAULTS_ARG="-f $FAULTS"
    fi
    local time_format='[log=time] Memory (KB): %M, System (s): %S User (s): %U | {"memory": %M, "system": %S, "user": %U}'
    # Figure 11 divides process memory by SHARDS, so every logical shard must have a
    # preallocated physical shard for that normalization to remain meaningful.
    (timeout "${timeout_s}s" /usr/bin/time -f "$time_format" ./kcensus --simulate-delays true -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" --duration "$DURATION" -i "$INGRESS" -t "$PER_PROPOSER_THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG -k "$KEYS" --skew "$SKEW" --shards "$SHARDS" --shard-pool "$SHARDS")>"$LOG_DIR/$pid.stdout" 2>"$LOG_DIR/$pid.stderr" &
    pids+=($!)
  done

  # Wait for all and capture failures
  failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done

  # Keep the output of a failed attempt: the next attempt writes to the same directory and would
  # otherwise erase the only evidence of saturation or bugs.
  if [ "$failed" -ne 0 ]; then
    local FAILED_DIR="$BASE_LOG_DIR/failed/$TITLE/attempt=${ATTEMPT:-1}"
    mkdir -p "$FAILED_DIR"
    cp -a "$LOG_DIR"/. "$FAILED_DIR"/ 2>/dev/null
    echo "Attempt ${ATTEMPT:-1} failed; output kept in $FAILED_DIR" >&2
  fi

  return $failed
}

# Resources
function run_resources() {
  local duration="10s"
  local throughput=1000
  local keys=100000
  local skew=0
  for configs in aws-random; do
    for writes in "${WRITES[@]}"; do
      for num_replicas in $(seq 3 2 31); do
        for algo in "${ALGOS[@]}"; do
          for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
            ATTEMPT="$attempt"
            if run "${configs}/${num_replicas}.toml" "$algo" "$writes" "$duration" exponential "$throughput" "" "$keys" "$skew" "$keys"; then
              break
            fi
            echo "Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${algo} on ${configs}/${num_replicas}.toml" >&2
            if [ "$attempt" -eq "$MAX_ATTEMPTS" ]; then
              echo "FAILED after ${MAX_ATTEMPTS} attempts: ${algo} on ${configs}/${num_replicas}.toml" >&2
              exit 1
            fi
          done
        done
      done
    done
  done
}

run_resources
