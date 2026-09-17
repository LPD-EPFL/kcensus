#!/usr/bin/env bash
#
# Local sweep over the batching configurations of the dependency layer.
#
# Every run is a full 7- or 31-replica deployment on this machine with the wide-area link
# delays simulated, so the absolute numbers are not a deployment's -- the comparison between
# variants at one setting is what this is for.
#
# Usage: ./bench-batching.sh [OUTPUT_DIR]
#
# Logs go to OUTPUT_DIR/<variant>/<the usual log path>, one directory per run, so a variant's
# tree can be read by graphs/logparser.py on its own. Completed runs are skipped on a rerun;
# delete a run's directory to redo it.

set -u
cd "$(dirname "$0")"
source ./lib.sh

OUT="${1:-./bench-logs}"
BIN=./target/release/kcensus
CONFIG_31="aws-random/31.toml"
TIME_FORMAT='[log=time] Memory (KB): %M | {"memory": %M, "system": %S, "user": %U}'
# Generous: a backstop for a hang, not the experiment's own deadline, which the binary
# enforces itself. The top rungs write a lot of log.
RUN_TIMEOUT=240

# The dependency layer's two knobs, plus EPaxos' single-shard mode. Ack batching is ignored
# for EPaxos (it unicasts each ack to one coordinator), so E2 and E4 leave it at the default.
declare -A VARIANT_ALGO=(
  [E1]=epaxos [E2]=epaxos [E3]=epaxos [E4]=epaxos
  [S1]=swift-paxos [S2]=swift-paxos [S3]=swift-paxos [S4]=swift-paxos
)
declare -A VARIANT_FLAGS=(
  [E1]="--batching=false"
  [E2]="--batching=true"
  [E3]="--batching=false --cross-shard-batching"
  [E4]="--batching=true --cross-shard-batching"
  [S1]="--batch-commands=false --batch-acks=false"
  [S2]="--batch-commands=true  --batch-acks=false"
  [S3]="--batch-commands=false --batch-acks=true"
  [S4]="--batch-commands=true  --batch-acks=true"
)

SUMMARY="${OUT}/summary.tsv"
started=$(date +%s)
ran=0 skipped=0 failed=0

# bench <variant> <configName> <configFile> <writes> <throughput> <skew> <conflicts>
function bench() {
  local variant="$1" configName="$2" configFile="$3" writes="$4" throughput="$5"
  local skew="$6" conflicts="$7"
  local algo="${VARIANT_ALGO[$variant]}"
  local duration="$DURATION" ingress=exponential speedup="$SPEEDUP" faults=""
  local keys="$KEYS" shards="$KEYS"

  local nb; nb="$(digits "$configName")"
  local per_proposer; per_proposer="$(per_proposer_rate "$throughput" "$nb")"
  local title; title="$(make_title)" || return 1
  local dir="${OUT}/${variant}/${title}"

  if [ -f "${dir}/.done" ]; then
    skipped=$((skipped + 1))
    return 0
  fi
  rm -rf "$dir" && mkdir -p "$dir"
  printf '%-4s %-22s t=%-6s skew=%-5s ' "$variant" "$configName" "$throughput" "$skew"

  pkill -x kcensus 2>/dev/null
  local conflictsArg=()
  [ -n "$conflicts" ] && conflictsArg=("--conflicts=$conflicts")
  local pids=() pid
  for pid in $(seq 0 $((nb - 1))); do
    ( timeout "${RUN_TIMEOUT}s" /usr/bin/time -f "$TIME_FORMAT" "$BIN" \
        --simulate-delays true -p "$pid" --config "configs/${configFile}" \
        -a "$algo" -w "$writes" --duration "$duration" -i "$ingress" \
        -t "$per_proposer" -s "$speedup" -k "$keys" --skew "$skew" --shards "$shards" \
        "${conflictsArg[@]}" ${VARIANT_FLAGS[$variant]} \
    ) > "${dir}/${pid}.stdout" 2> "${dir}/${pid}.stderr" &
    pids+=($!)
  done

  local bad=0
  for pid in "${pids[@]}"; do wait "$pid" || bad=1; done
  ran=$((ran + 1))
  if [ "$bad" -eq 0 ]; then
    touch "${dir}/.done"
    echo "ok"
    printf '%s\t%s\t%s\t%s\t%s\tok\t%s\n' \
      "$variant" "$algo" "$configName" "$throughput" "$skew" "$dir" >> "$SUMMARY"
  else
    failed=$((failed + 1))
    echo "NOT SUSTAINED"
    printf '%s\t%s\t%s\t%s\t%s\tfailed\t%s\n' \
      "$variant" "$algo" "$configName" "$throughput" "$skew" "$dir" >> "$SUMMARY"
  fi
  return 0
}

mkdir -p "$OUT"
[ -f "$SUMMARY" ] || printf 'variant\talgo\tconfig\tthroughput\tskew\tstatus\tdir\n' > "$SUMMARY"

echo "=== Block 1: exp-1 settings (no conflicts, writes=1, ${THROUGHPUT} req/s) ==="
for configName in "${EXP1_CONFIGS[@]}"; do
  for variant in E1 E2 E4 S1 S3; do
    bench "$variant" "$configName" "${configName}.toml" 1 "$THROUGHPUT" "$SKEW" ""
  done
done

echo "=== Block 2: exp-3 settings (${CONFIG_31}, no conflicts, writes=1, ${THROUGHPUT} req/s) ==="
for variant in E1 E2 E4 S1 S3; do
  bench "$variant" "$CONFIG_31" "$CONFIG_31" 1 "$THROUGHPUT" "$SKEW" ""
done

echo "=== Block 3: exp-5 settings (skew 0.99, ${EXP5_CDF_THROUGHPUT} req/s) ==="
for variant in E1 E2 E3 E4 S1 S2 S3 S4; do
  bench "$variant" "$EXP5_CONFIG" "${EXP5_CONFIG}.toml" "$EXP5_WRITES" \
    "$EXP5_CDF_THROUGHPUT" 0.99 true
done

echo "=== Block 4: exp-5 under load, EPaxos ==="
for throughput in 2000 2500 3000; do
  for variant in E1 E2 E3 E4; do
    bench "$variant" "$EXP5_CONFIG" "${EXP5_CONFIG}.toml" "$EXP5_WRITES" "$throughput" 0.99 true
  done
done

LOAD_RUNGS=(15000 20000 30000 40000 50000)

echo "=== Block 5: exp-5 under load, SwiftPaxos, skew 0.99 ==="
for throughput in "${LOAD_RUNGS[@]}"; do
  for variant in S1 S3 S4; do
    bench "$variant" "$EXP5_CONFIG" "${EXP5_CONFIG}.toml" "$EXP5_WRITES" "$throughput" 0.99 true
  done
done

# The other skew exp-5 sweeps (EXP5_LOAD_SKEWS). Uniform keys still conflict occasionally
# over 100000 of them, but rarely enough that EPaxos reaches these rungs too.
echo "=== Block 6: exp-5 under load, skew 0 ==="
for throughput in "${LOAD_RUNGS[@]}"; do
  for variant in E1 E2 E4 S1 S3; do
    bench "$variant" "$EXP5_CONFIG" "${EXP5_CONFIG}.toml" "$EXP5_WRITES" "$throughput" 0 true
  done
done

echo
echo "=== Done in $(( ($(date +%s) - started) / 60 )) min: ${ran} run, ${skipped} skipped, ${failed} not sustained ==="
echo "Logs under ${OUT}, summary in ${SUMMARY}"
