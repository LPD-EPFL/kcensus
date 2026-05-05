#!/usr/bin/env bash

CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(kcensus "weak-replication" "swift-paxos" pando epaxos "multi-paxos" paxos)
ALGOS=(no-replication ${REPLICATED_ALGOS[@]})
CONFIGS=(aws-europe-7-alt.toml aws-north-america-7.toml aws-world-ring-13.toml) # aws-europe-7.toml aws-world-ring-9.toml
YCSB=(1 0.5 0.05)
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

function run() {
  local CONFIG="$1"
  local ALGO="$2"
  local WRITES="$3"
  local REQUESTS="$4"
  local INGRESS="$5"
  local THROUGHPUT="$6"
  local FAULTS="$7"
  local TITLE="c=$CONFIG/a=$ALGO/w=$WRITES/r=$REQUESTS/i=$INGRESS/t=$THROUGHPUT/s=$SPEEDUP/f=$FAULTS/no-conflicts"
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
    (/usr/bin/time -f "$time_format" target/release/kcensus -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG)>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
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
        python3 2-cdfs.py -c "$config" -w "$writes" -r "$REQUESTS" -i round-robin -t 0 -s "$SPEEDUP"
      )
    done
  done
}

# Latency under load
function exp-2() {
  local LOADS=(0.05 0.1) # req/s per client
  for writes in "${YCSB[@]}"; do
    for config in aws-world-ring-13.toml; do # "${CONFIGS[@]}"; do
      for load in "${LOADS[@]}"; do
        for algo in "${ALGOS[@]}"; do
          run "$config" "$algo" "$writes" "$REQUESTS" exponential "$load"
        done
        (
          cd graphs &&
          source env.sh >/dev/null 2>&1 &&
          python3 1-bars.py -c "$config" -w "$writes" -r "$REQUESTS" -i exponential -t "$load" -s "$SPEEDUP" &&
          python3 2-cdfs.py -c "$config" -w "$writes" -r "$REQUESTS" -i exponential -t "$load" -s "$SPEEDUP"
        )
      done
    done
  done
}

# Scalability
function exp-3() {
  local requests=10
  for configs in aws-random aws-from-paris; do
    for writes in "${YCSB[@]}"; do
      for num_replicas in $(seq 3 2 31); do
        for algo in "${ALGOS[@]}"; do
          run "${configs}/${num_replicas}.toml" "$algo" "$writes" "$requests" round-robin 0
        done
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 3-scalability.py -c "${configs}/@.toml" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP"
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
  local config=aws-world-ring-9.toml
  local writes=1
  local requests=10
  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in "" $(all_faults "$(digits "$config")"); do
      run "$config" "$algo" $writes $requests round-robin 0 "$faults"
    done
  done
  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 4-faults.py -c "$config" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP"
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
  local requests=1000
  for configs in aws-random aws-from-paris; do
    for writes in "${YCSB[@]}"; do
      for num_replicas in $(seq 3 2 31); do
        for algo in "${ALGOS[@]}"; do
          run "${configs}/${num_replicas}.toml" "$algo" "$writes" "$((requests / num_replicas))" round-robin 0
        done
      done
      (
        cd graphs &&
        source env.sh >/dev/null 2>&1 &&
        python3 6-network.py -c "${configs}/@.toml" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP" &&
        python3 7-cpu-mem.py -c "${configs}/@.toml" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP"
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
#python3 4-faults.py -c=aws-world-ring-9.toml -w=1 -r=10 -i=round-robin -t=0
#python3 2-cdfs-merged.py
#python3 3-scalability-merged.py
#python3 5-propagation.py
#python3 6-network.py -r 1000 -c aws-random/@.toml -s 10000000 -w 1 -t=0
#python3 7-cpu-mem.py -r 1000 -c aws-random/@.toml -s 10000000 -w 1 -t=0
