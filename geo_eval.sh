#!/usr/bin/env bash

set -e

BASE_LOG_DIR="./logs"
REPLICATED_ALGOS=(k-census e-paxos multi-paxos paxos weak-replication)
ALGOS=(no-replication "${REPLICATED_ALGOS[@]}")
YCSB=(1 0.5 0.05)
REQUESTS=100
SPEEDUP=1

declare -A CONFIGS
CONFIGS["aws-europe-7"]="deployment/terraform/regions/europe-7.tfvars"
CONFIGS["aws-europe-3"]="deployment/terraform/regions/europe-3.tfvars"
CONFIGS["aws-europe-2"]="deployment/terraform/regions/europe-2.tfvars"
CONFIGS["aws-north-america-7"]="deployment/terraform/regions/north-america-7.tfvars"
CONFIGS["aws-world-ring-13"]="deployment/terraform/regions/world-ring-13.tfvars"
CONFIGS["aws-world-ring-9"]="deployment/terraform/regions/world-ring-9.tfvars"

function digits() {
  echo "$1" | tr -d -c 0-9
}

function provision() {
  local varFile="$1"
  local expId="$2"
  echo "--> Provisioning infrastructure defined in ${varFile} for experiment ID: ${expId}..."
  (
    cd deployment/terraform
    terraform init -upgrade
    terraform plan -var-file="../../${varFile}" -var="experiment_id=${expId}"
    terraform apply -var-file="../../${varFile}" -var="experiment_id=${expId}" -auto-approve
  )
  echo "--> Infrastructure is UP for Exp ID ${expId}. Waiting for instances to be fully ready... (60s)"
  sleep 60
}

function deploy() {
  local expId="$1"
  local inventoryFile="inventory-${expId}.ini"
  echo "--> Deploying code and config using inventory: ${inventoryFile}..."
  (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" deploy_and_configure.yml
  )
  echo "--> Deployment and configuration complete."
}

function destroy() {
  local varFile="$1"
  local expId="$2"
  echo "--> Destroying infrastructure defined in ${varFile}..."
  (
    cd deployment/terraform
    terraform destroy -var-file="../../${varFile}" -var="experiment_id=${expId}" -auto-approve
  )
  echo "--> Infrastructure is DOWN."
}

function run() {
  local expId="$1"
  local configName="$2"
  local algo="$3"
  local writes="$4"
  local requests="$5"
  local ingress="$6"
  local throughput="$7"
  local faults="${8:-}"


  local inventoryFile="inventory-${expId}.ini"
  local title="c=${configName}/a=${algo}/w=${writes}/r=${requests}/i=${ingress}/t=${throughput}/s=${SPEEDUP}/f=${faults}"
  local resultPath="${ABSOLUTE_BASE_LOG_DIR}/${title}"
  mkdir -p "${resultPath}"

  echo "--> RUNNING: ${title} (Exp ID: ${expId})"

  (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" run_experiment.yml \
      -e "algo=${algo}" \
      -e "writes=${writes}" \
      -e "requests=${requests}" \
      -e "ingress=${ingress}" \
      -e "throughput=${throughput}" \
      -e "speedup=${SPEEDUP}" \
      -e "faults=${faults}" \
      -e "result_path=${resultPath}"
  )
  echo "--> COMPLETED. Logs are in ${resultPath}"
}

function cleanup_processes() {
  local expId="$1"
  local inventoryFile="inventory-${expId}.ini"
  echo "--> Cleaning up stray processes on all nodes..."
  (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" kill_processes.yml
  )
  echo "--> Cleanup complete."
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

# No load, pure latency
# exp-1 <=> 7.1
function exp-1() {
  echo "--- Starting Experiment 1: Pure Latency ---"
  for configName in "aws-europe-7" "aws-north-america-7" "aws-world-ring-13"; do

    local EXPERIMENT_ID="exp-1-$configName"
    local varFile="${CONFIGS[$configName]}"
    provision "$varFile" "$EXPERIMENT_ID"
    deploy "$EXPERIMENT_ID"

    for writes in 1; do
      for algo in "${ALGOS[@]}"; do
        run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$REQUESTS" "round-robin" 0
      done
    done

    destroy "$varFile" "$EXPERIMENT_ID"
  done

  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 parse_geo_logs.py &&
    python3 1-bars-merged.py -g 1
  )

  echo "--- Finished Experiment 1 ---"
}

# Latency under load
# exp-2 <=> 7.3
function exp-2() {
  echo "--- Starting Experiment 2: Latency under load ---"

  local configName="aws-world-ring-13"
  local EXPERIMENT_ID="exp-2"
  local varFile="${CONFIGS[$configName]}"
  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  local LOADS=(0.1 0.2) # req/s per client

  for writes in 1; do
      for load in "${LOADS[@]}"; do
        for algo in "${ALGOS[@]}"; do
          run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$REQUESTS" "exponential" "$load"
        done
      done
  done

  destroy "$varFile" "$EXPERIMENT_ID"

  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 parse_geo_logs.py &&
    python3 2-cdfs-merged.py -g 1
  )

  echo "--- Finished Experiment 2 ---"
}


# Faults
# exp-4 <=> 7.2
function exp-4() {
  echo "--- Starting Experiment 4: Faults ---"

  local configName="aws-world-ring-9"
  local EXPERIMENT_ID="exp-4"
  local varFile="${CONFIGS[$configName]}"
  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  local writes=1
  local requests=10

  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in "" $(all_faults "$(digits "$configName")"); do
      run "$EXPERIMENT_ID" "$configName" "$algo" $writes $requests round-robin 0 "$faults"
    done
  done

  destroy "$varFile" "$EXPERIMENT_ID"

  (
    cd graphs &&
    source env.sh >/dev/null 2>&1 &&
    python3 parse_geo_logs.py &&
    python3 4-faults.py -c "$configName" -w "$writes" -r "$requests" -i round-robin -t 0 -s "$SPEEDUP" -g 1
  )

  echo "--- Finished Experiment 4 ---"
}


function main() {
  echo "Starting Geo-Replicated Evaluation."

  mkdir -p "${BASE_LOG_DIR}"
  ABSOLUTE_BASE_LOG_DIR="$(cd "${BASE_LOG_DIR}" && pwd)"

}

main