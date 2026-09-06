#!/usr/bin/env bash
#
# Run every experiment locally, with all replicas as processes on this machine and link delays
# simulated from the configs in `configs/`. Same experiments, same algorithms, same log-path
# schema as geo_eval.sh -- see lib.sh, which both scripts share so they cannot drift apart.
#
# Two things differ from the AWS runs, and both are visible in the log path:
#   * the log root is ./local-logs, never ./logs, so a local run can never overwrite AWS results;
#   * throughput is divided by (f+1)/2, because one machine cannot sustain the wide-area rate.
#     The path records the real reduced value (t=500 for 7 replicas, t=125 for 31), so `plot.sh
#     --local` has to apply the same arithmetic to find it.

set -e

source "$(dirname "$0")/lib.sh"

BASE_LOG_DIR="./local-logs"
BIN=./target/x86_64-unknown-linux-musl/release/kcensus
GRAPH_BENCH=./target/x86_64-unknown-linux-musl/release/graph_bench
TIME_FORMAT='[log=time] Memory (KB): %M, System (s): %S User (s): %U | {"memory": %M, "system": %S, "user": %U}'


function build_binaries() {
  rustup target add x86_64-unknown-linux-musl
  cargo build --target x86_64-unknown-linux-musl --release
}

# run <configName> <configFile> <algo> <writes> <duration> <ingress> <speedup> [faults] [keys] [skew] [shards] [conflicts]
#
# <configName> is what goes in the log path (matching geo_eval.sh exactly); <configFile> is the
# TOML under configs/. They differ when the log-path name omits a suffix and when an experiment
# needs its own latency snapshot.
function run_one() {
  local configName="$1" configFile="$2" algo="$3" writes="$4" duration="$5" ingress="$6"
  local speedup="$7" faults="${8:-}" keys="${9:-${KEYS}}" skew="${10:-${SKEW}}"
  local shards="${11:-${SHARDS}}" conflicts="${12:-}"

  local nb; nb="$(digits "$configName")"
  local throughput; throughput="$(local_throughput "$nb")"
  local per_proposer; per_proposer="$(per_proposer_rate "$throughput" "$nb")"
  local nonvoting; nonvoting="$(get_nonvoting "$configName" "$algo")"
  # The binary needs warmup+duration+sustain = 1.4375x duration and its deadlock detector fires
  # at 2.16x, but startup and connection establishment cost a fixed amount that does not scale
  # with the run. 30s + 3x duration covers both, and still leaves the deadlock detector to fire
  # first so a stuck run reports which shard hung instead of being killed silently.
  local timeout_s=$(( 30 + 3 * ${duration%s} ))

  local title; title="$(make_title)"
  local logDir="${BASE_LOG_DIR}/${title}"
  mkdir -p "$logDir"

  echo "--> RUNNING: ${title}"

  local attempt
  for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
    # Match the process *name* exactly: `pkill -f kcensus` would also match any shell whose
    # command line merely mentions the repo path, including this script's own caller.
    pkill -x kcensus 2>/dev/null || true
    local pids=() pid
    for pid in $(seq 0 $((nb - 1))); do
      local faultsArg=() nonvotingArg=() conflictsArg=()
      [ -n "$faults" ]     && faultsArg=(-f "$faults")
      [ -n "$nonvoting" ]  && nonvotingArg=(-v "$nonvoting")
      [ -n "$conflicts" ]  && conflictsArg=("--conflicts=$conflicts")
      ( timeout "${timeout_s}s" /usr/bin/time -f "$TIME_FORMAT" "$BIN" \
          --simulate-delays true -p "$pid" --config "configs/${configFile}" \
          -a "$algo" -w "$writes" --duration "$duration" -i "$ingress" \
          -t "$per_proposer" -s "$speedup" -k "$keys" --skew "$skew" --shards "$shards" \
          "${nonvotingArg[@]}" "${faultsArg[@]}" "${conflictsArg[@]}" \
      ) > "${logDir}/${pid}.stdout" 2> "${logDir}/${pid}.stderr" &
      pids+=($!)
    done

    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
    [ "$failed" -eq 0 ] && return 0

    # Keep the failed attempt: the next one overwrites this directory, and a deadlock or panic
    # would otherwise leave no evidence at all.
    local failedDir="${BASE_LOG_DIR}/failed/${title}/attempt=${attempt}"
    mkdir -p "$failedDir" && cp -a "${logDir}/." "${failedDir}/" 2>/dev/null || true
    echo "--> Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${algo} on ${configName}; output kept in ${failedDir}" >&2
    if [ "$attempt" -eq "${MAX_ATTEMPTS}" ]; then
      echo "--> FAILED after ${MAX_ATTEMPTS} attempts: ${title}" >&2
      exit 1
    fi
  done
}

# --- Experiment 1: end-to-end latency (Figures 1 and 7) ---
function exp-1() {
  echo "--- Experiment 1: end-to-end latency (local) ---"
  local configName algo
  for configName in "${EXP1_CONFIGS[@]}"; do
    for algo in "${ALGOS[@]}"; do
      run_one "$configName" "${configName}.toml" "$algo" 1 "$(local_duration "$(digits "$configName")")s" exponential "$SPEEDUP"
    done
  done
  echo "--- Finished Experiment 1 ---"
}

# --- Experiment 2: impact of failures (Figure 8) ---
function exp-2() {
  echo "--- Experiment 2: impact of failures (local) ---"
  local algo faults
  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in "" $(all_faults "$(digits "$EXP2_CONFIG")" "$(get_nonvoting "$EXP2_CONFIG" "$algo")"); do
      run_one "$EXP2_CONFIG" "${EXP2_CONFIG}.toml" "$algo" 1 "$(local_duration "$(digits "$EXP2_CONFIG")")s" exponential "$SPEEDUP" "$faults"
    done
  done
  echo "--- Finished Experiment 2 ---"
}

# --- Experiment 3: scalability and optimization time (Figures 9 and 12) ---
function exp-3() {
  echo "--- Experiment 3: scalability + optimization time (local) ---"
  local type n algo
  for type in "${EXP3_TYPES[@]}"; do
    for n in "${EXP3_SIZES[@]}"; do
      local configFile="exp-3/${type}/${n}.toml"
      for algo in "${ALGOS[@]}"; do
        # A quarter of an AWS run's requests: still >=2500 samples at every size, against ~1250
        # with a fixed window. Half would be nicer statistically but costs 1.7h instead of 1.0h,
        # and a local run is stable enough that the extra samples buy little.
        run_one "${type}/${n}.toml" "$configFile" "$algo" 1 "$(local_duration "$n" 4)s" exponential "$SPEEDUP"
      done
      # One propagation measurement per deployment (Figure 12).
      local graphDir="${BASE_LOG_DIR}/c=${type}/${n}.toml"
      mkdir -p "$graphDir"
      echo "--> RUNNING Graph Bench: c=${type}/${n}.toml"
      "$GRAPH_BENCH" --config "configs/${configFile}" --fault-count 0 -w 50 -s 200 \
        > "${graphDir}/graph_bench.stdout" 2> "${graphDir}/graph_bench.stderr"
    done
  done
  echo "--- Finished Experiment 3 ---"
}

# --- Experiment 4: resource consumption (Figures 10 and 11) ---
# Note: unlike the others this asks for the *full* request count (sample divisor 1), not half.
# Figure 11 plots total CPU time, and compute is essentially proportional to requests processed:
# at 31 replicas a 10s local window would issue an eighth of the original run's requests, and the
# shortfall grows with n, bending the very curve the figure is meant to show.
function exp-4() {
  echo "--- Experiment 4: resource consumption (local) ---"
  local n algo configFile
  for n in "${EXP4_SIZES[@]}"; do
    configFile="exp-4/${EXP4_TYPE}/${n}.toml"
    for algo in "${ALGOS[@]}"; do
      run_one "${EXP4_TYPE}/${n}.toml" "$configFile" "$algo" 1 "$(local_duration "$n" 1)s" exponential "$EXP4_SPEEDUP"
    done
  done
  echo "--- Finished Experiment 4 ---"
}

function main() {
  if [[ $# -eq 0 ]]; then show_help; exit 0; fi

  if ! command -v /usr/bin/time >/dev/null 2>&1; then
    echo "/usr/bin/time (GNU time) is required: the resource figures parse its output." >&2
    exit 1
  fi
  mkdir -p "$BASE_LOG_DIR"
  build_binaries

  case "$1" in
    "exp-1") exp-1 ;;
    "exp-2") exp-2 ;;
    "exp-3") exp-3 ;;
    "exp-4") exp-4 ;;
    "all")   exp-1; exp-2; exp-3; exp-4 ;;
    "help"|"-h"|"--help") show_help ;;
    *) echo "Error: Unknown command '$1'"; echo; show_help; exit 1 ;;
  esac
}

main "$@"
