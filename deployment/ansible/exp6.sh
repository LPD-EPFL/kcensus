#!/usr/bin/env bash


CASSANDRA_BASE_PORT="9042"
BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(k-census e-paxos multi-paxos paxos weak-replication)
ALGOS=(no-replication ${REPLICATED_ALGOS[@]})
CONFIGS=(aws-europe-7-alt.toml aws-north-america-7.toml aws-world-ring-13.toml)
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
    local FAULTS_ARG=""
    if [[ "$FAULTS" != "" ]]; then
       FAULTS_ARG="-f $FAULTS"
    fi
    local time_format='[log=time] Memory (KB): %M, System (s): %S User (s): %U | {"memory": %M, "system": %S, "user": %U}'
    (/usr/bin/time -f "$time_format" ./kcensus --simulate-delays -p "$pid" --config "configs/$CONFIG" $CASSANDRA_ARG -a "$ALGO" -w "$WRITES" -r "$REQUESTS" -i "$INGRESS" -t "$THROUGHPUT" -s "$SPEEDUP" $FAULTS_ARG)>"$LOG_DIR/$pid.stdout" 2>>"$LOG_DIR/$pid.stderr" &
  done
  wait
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
    done
  done
}

exp-6