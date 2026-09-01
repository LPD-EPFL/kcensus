#!/usr/bin/env bash

CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(kcensus "weak-replication" "swift-paxos" pando epaxos "multi-paxos" paxos)
ALGOS=(no-replication ${REPLICATED_ALGOS[@]})
CONFIGS=(aws-europe-8.toml aws-north-america-7.toml aws-east-asia-9.toml)
YCSB=(1 0.5 0.05)
DURATION=10s
THROUGHPUT=1000 # Total req/s, split evenly between the proposers.
SPEEDUP=1
KEYS=10000
SKEW=0
SHARDS=$KEYS

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

function stop_cassandra() {
  docker ps -a -q --filter="name=cassandra" | xargs docker rm -f
}

# run <config> <algo> <writes> <duration> <ingress> <throughput> [faults] [keys] [skew] [shards]
#
# <throughput> is the total target req/s: like geo_eval.sh, it is what the title records,
# while each proposer is started with its own share of it. The log directory has to match
# the path graphs/logparser.py rebuilds, so any parameter added here has to be added there.
function run() {
  local CONFIG="$1"
  local ALGO="$2"
  local WRITES="$3"
  local DURATION="$4"
  local INGRESS="$5"
  local THROUGHPUT="$6"
  local FAULTS="${7:-}"
  local KEYS="${8:-${KEYS}}"
  local SKEW="${9:-${SKEW}}"
  local SHARDS="${10:-${SHARDS}}"

  local TITLE="c=$CONFIG/a=$ALGO/w=$WRITES/d=$DURATION/i=$INGRESS/t=$THROUGHPUT/s=$SPEEDUP/f=$FAULTS/k=$KEYS/skew=$SKEW/shards=$SHARDS"
  local LOG_DIR="$BASE_LOG_DIR/$TITLE/"
  mkdir -p "$LOG_DIR"
  killall kcensus 2>/dev/null
  local NB=$(digits "$CONFIG")
  local PER_PROPOSER_THROUGHPUT=$((THROUGHPUT / NB))
#  if [[ "${CASSANDRA,,}" != "false" && "$CASSANDRA" != "0" ]]; then
#    start_cassandra "$NB"
#  fi
  echo "Starting $TITLE"
  cargo build -r 2>"$LOG_DIR/build.stderr" || return 1
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
    (/usr/bin/time -f "$time_format" target/release/kcensus -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" --duration "$DURATION" -i "$INGRESS" -t "$PER_PROPOSER_THROUGHPUT" -s "$SPEEDUP" -k "$KEYS" --skew "$SKEW" --shards "$SHARDS" $FAULTS_ARG)>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
  done
  wait
}

# Arguments identifying a run for the plotting scripts, mirroring `run`'s title.
function plot_args() {
  local CONFIG="$1"
  local WRITES="$2"
  local THROUGHPUT="${3:-${THROUGHPUT}}"
  local SKEW="${4:-${SKEW}}"
  echo -c "$CONFIG" -w "$WRITES" -d "$DURATION" -i exponential -t "$THROUGHPUT" -s "$SPEEDUP" -k "$KEYS" --skew "$SKEW" --shards "$SHARDS"
}

# No load, pure latency
function exp-1() {
  for writes in "${YCSB[@]}"; do
    for config in "${CONFIGS[@]}"; do
      for algo in "${ALGOS[@]}"; do
        run "$config" "$algo" "$writes" "$DURATION" exponential "$THROUGHPUT"
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 1-bars.py $(plot_args "$config" "$writes") &&
        python3 2-cdfs.py $(plot_args "$config" "$writes")
      )
    done
  done
}

# Latency under contention
function exp-2() {
  for writes in "${YCSB[@]}"; do
    for config in aws-europe-8.toml; do # "${CONFIGS[@]}"; do
      for skew in 0.5 1 2; do # 0 has run as part of exp-1
        for algo in "${ALGOS[@]}"; do
          run "$config" "$algo" "$writes" "$DURATION" exponential "$THROUGHPUT" "" "$KEYS" "$skew"
        done
        (
          cd graphs &&
          source env.sh >/dev/null 2>&1 &&
          python3 1-bars.py $(plot_args "$config" "$writes" "$THROUGHPUT" "$skew") &&
          python3 2-cdfs.py $(plot_args "$config" "$writes" "$THROUGHPUT" "$skew")
        )
      done
    done
  done
}

# Scalability
function exp-3() {
  for configs in aws-random aws-from-paris; do
    for writes in "${YCSB[@]}"; do
      for num_replicas in $(seq 3 2 31); do
        for algo in "${ALGOS[@]}"; do
          run "${configs}/${num_replicas}.toml" "$algo" "$writes" "$DURATION" exponential "$THROUGHPUT"
        done
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 3-scalability.py $(plot_args "${configs}/@.toml" "$writes")
      )
    done
  done
}

function all_faults() {
python3 - <<END
from itertools import combinations
REPLICAS=$1
FROM="$2"
TO="$3"
MAJORITY=REPLICAS // 2
done = False
should_yield = FROM == ''
for r in range(1, MAJORITY + 1):
    if done: break
    for comb in combinations(range(REPLICAS), r):
        formatted = ','.join(map(str, comb))
        if formatted == FROM: should_yield = True
        if formatted == TO and TO != '': done = True; break;
        if should_yield:
          print(formatted, end=' ')
END
}

# Faults
function exp-4() {
  local config=aws-europe-8.toml
  local writes=1
  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in "" $(all_faults "$(digits "$config")"); do
      run "$config" "$algo" $writes "$DURATION" exponential "$THROUGHPUT" "$faults"
    done
  done
  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 4-faults.py $(plot_args "$config" "$writes")
  )
}

# Propagation
function exp-5() {
  for configs in aws-random aws-from-paris; do
    for num_replicas in $(seq 3 2 31); do
      local TITLE="c=$configs/${num_replicas}.toml"
      local LOG_DIR="$BASE_LOG_DIR/$TITLE/"
      local STDOUT="${LOG_DIR}/graph_bench.stdout"
      local STDERR="${LOG_DIR}/graph_bench.stderr"
      mkdir -p "$LOG_DIR"
      echo "" >"$STDOUT" 2>>"$STDERR"
      for num_faults in 0; do # $(seq 0 $(((num_replicas / 2) < 2 ? (num_replicas / 2) : 2))); do
        echo "Running graph bench on $TITLE with $num_faults faults"
        cargo run --bin graph_bench -r -- --config "configs/${configs}/${num_replicas}.toml" --fault-count "$num_faults">>"$STDOUT" 2>>"$STDERR"
      done
    done
  done
  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 5-propagation.py
  )
}

# Resources
function exp-6() {
  SPEEDUP=10000000 # latency precision does not matter
  for configs in aws-random aws-from-paris; do
    for writes in "${YCSB[@]}"; do
      for num_replicas in $(seq 3 2 31); do
        for algo in "${ALGOS[@]}"; do
          run "${configs}/${num_replicas}.toml" "$algo" "$writes" "$DURATION" exponential "$THROUGHPUT"
        done
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 6-network.py $(plot_args "${configs}/@.toml" "$writes") &&
        python3 7-cpu-mem.py $(plot_args "${configs}/@.toml" "$writes")
      )
    done
  done
}

exp-1
exp-2
exp-3
exp-4
exp-5
exp-6

#python3 1-bars-merged.py
#python3 2-cdfs-merged.py
#python3 3-scalability-merged.py
#python3 5-propagation.py
