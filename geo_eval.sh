#!/usr/bin/env bash

set -e

# Infrastructure currently provisioned. An abort (exhausted retries, `set -e`, or Ctrl-C) must
# still tear it down: `run` aborting with `exit` would otherwise skip the `destroy` at the end of
# the calling experiment and leave 7-31 instances billing.
CURRENT_VAR_FILE=""
CURRENT_EXP_ID=""

# Bounded retries for a failed experiment run. A failure here is rare -- a kcensus panic (e.g.
# the deadlock detector), the 60s timeout, or a transient SSH/node problem -- so exhausting all
# attempts means something is genuinely wrong and the script aborts rather than looping forever.
# The delay is applied *before* the retry: it gives a transient cause time to clear and gives
# `pkill` time to actually reap the process that would otherwise still hold port 8000.
# MAX_ATTEMPTS comes from lib.sh; the delays are AWS-only (a local retry has nothing to wait for).
RETRY_DELAYS=(0 60 120 300)   # before attempts 2, 3, 4 and 5 respectively

# Experiment definitions shared with eval.sh (algorithms, workload, configs, log-path schema).
source "$(dirname "$0")/lib.sh"

BASE_LOG_DIR="./logs"

declare -A CONFIGS
CONFIGS["aws-ring-7"]="deployment/terraform/regions/ring-7.tfvars"
CONFIGS["aws-north-america-7"]="deployment/terraform/regions/north-america-7.tfvars"
CONFIGS["aws-europe-7"]="deployment/terraform/regions/europe-7.tfvars"
CONFIGS["aws-east-asia-7"]="deployment/terraform/regions/east-asia-7.tfvars"
CONFIGS["aws-exp-4"]="deployment/terraform/regions/one.tfvars"
# Old
CONFIGS["aws-east-asia-9"]="deployment/terraform/regions/east-asia-9.tfvars"
CONFIGS["aws-europe-8"]="deployment/terraform/regions/europe-8.tfvars"
CONFIGS["old/aws-europe-7"]="deployment/terraform/regions/europe-7.tfvars"
CONFIGS["old/aws-europe-3"]="deployment/terraform/regions/europe-3.tfvars"
CONFIGS["old/aws-europe-2"]="deployment/terraform/regions/europe-2.tfvars"

# Namespace an AWS deployment for concurrent users of the same account. Keeping the experiment
# name first makes resources easy to associate with the README and preserves the old IDs when no
# --run-id was supplied.
function experiment_id() {
  local base="$1"
  if [ -n "${KCENSUS_RUN_ID:-}" ]; then
    echo "${base}-${KCENSUS_RUN_ID}"
  else
    echo "$base"
  fi
}


function activate_env() {
  pushd graphs >/dev/null
  source env.sh >/dev/null 2>&1
  popd >/dev/null
}

function provision() {
  local varFile="$1"
  local expId="$2"
  echo "--> Provisioning infrastructure defined in ${varFile} for experiment ID: ${expId}..."
  # Recorded *before* the apply, not after: an interrupt part-way through leaves instances
  # running, and the teardown has to know about them. Terraform's state covers whatever it
  # got to create, so destroying against it is enough.
  CURRENT_VAR_FILE="${varFile}"
  CURRENT_EXP_ID="${expId}"
  (
    cd deployment/terraform
    terraform init -upgrade
    # terraform plan -var-file="../../${varFile}" -var="experiment_id=${expId}"
    terraform apply -parallelism=50 -var-file="../../${varFile}" -var="experiment_id=${expId}" -auto-approve
  )
  echo "--> Infrastructure is UP for Exp ID ${expId}; VMs might still be booting."
}

function deploy() {
  local expId="$1"
  local inventoryFile="inventory-${expId}.ini"
  echo "--> Deploying code and preparing nodes using inventory: ${inventoryFile}..."
  (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" 01-prepare-nodes.yml
    ansible-playbook -i "${inventoryFile}" 02-generate-configs.yml
  )
  echo "--> Deployment and configuration complete."
}

function destroy() {
  local varFile="$1"
  local expId="$2"
  echo "--> Destroying infrastructure defined in ${varFile}..."
  (
    cd deployment/terraform
    terraform destroy -parallelism=50 -var-file="../../${varFile}" -var="experiment_id=${expId}" -auto-approve
  )
  CURRENT_VAR_FILE=""
  CURRENT_EXP_ID=""
  echo "--> Infrastructure is DOWN."
}

# Tear down whatever is still provisioned when the script exits non-zero or is interrupted.
function teardown_on_abort() {
  local code=$?
  trap - EXIT INT TERM
  if [ "${code}" -ne 0 ] && [ -n "${CURRENT_EXP_ID}" ]; then
    echo "--> Aborting: tearing down ${CURRENT_EXP_ID} before exit..." >&2
    destroy "${CURRENT_VAR_FILE}" "${CURRENT_EXP_ID}" || echo \
      "--> WARNING: teardown FAILED. Destroy manually:" \
      "./geo_eval.sh destroy ${CURRENT_VAR_FILE} ${CURRENT_EXP_ID}" >&2
  fi
  exit "${code}"
}

function run() {
  local expId="$1"
  local configName="$2"
  local algo="$3"
  local writes="$4"
  local duration="$5"
  local ingress="$6"
  local throughput="$7"
  local faults="${8:-}"
  local keys="${9:-${KEYS}}"
  local skew="${10:-${SKEW}}"
  local shards="${11:-${SHARDS}}"
  local conflicts="${12:-}"
  local speedup="${SPEEDUP}"

  local nonvoting="$(get_nonvoting "$configName" "$algo")"

  local inventoryFile="inventory-${expId}.ini"
  local title; title="$(make_title)"
  local resultPath="${ABSOLUTE_BASE_LOG_DIR}/${title}"
  mkdir -p "${resultPath}"

  echo "--> RUNNING: ${title} (Exp ID: ${expId})"

  local proposer_count="$(digits "$configName")"
  local per_proposer_throughput; per_proposer_throughput="$(per_proposer_rate "$throughput" "$proposer_count")"

  local attempt
  for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
    if (
      cd deployment/ansible
      ansible-playbook -i "${inventoryFile}" 03-run-experiment.yml \
        -e "algo=${algo}" \
        -e "writes=${writes}" \
        -e "duration=${duration}" \
        -e "ingress=${ingress}" \
        -e "throughput=${per_proposer_throughput}" \
        -e "speedup=${SPEEDUP}" \
        -e "faults=${faults}" \
        -e "keys=${keys}" \
        -e "skew=${skew}" \
        -e "shards=${shards}" \
        -e "nonvoting=${nonvoting}" \
        -e "conflicts=${conflicts}" \
        -e "result_path=${resultPath}" \
        -e "failed_path=${ABSOLUTE_BASE_LOG_DIR}/failed/${title}/attempt=${attempt}"
    ); then
      break
    fi
    echo "--> Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${algo} on ${configName} (faults=${faults})" >&2
    if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
      echo "--> FAILED after ${MAX_ATTEMPTS} attempts: ${title}" >&2
      exit 1
    fi
    # A crashed run can leave a kcensus process holding port 8000, which would make every
    # further attempt fail too.
    cleanup_processes "${expId}"
    retry_backoff "${attempt}"
  done
  echo "--> COMPLETED. Logs are in ${resultPath}"
}

# Wait before the retry that follows a failed attempt.
function retry_backoff() {
  local attempt="$1"
  local delay="${RETRY_DELAYS[$((attempt - 1))]}"
  if [ "${delay}" -gt 0 ]; then
    echo "--> Waiting ${delay}s before attempt $((attempt + 1))/${MAX_ATTEMPTS}..."
    sleep "${delay}"
  fi
}

function cleanup_processes() {
  local expId="$1"
  local inventoryFile="inventory-${expId}.ini"
  echo "--> Cleaning up stray processes on all nodes..."
  # Best-effort. This runs between retries, and whatever is being retried may be a transient SSH
  # problem that makes this playbook fail too. Aborting here (`set -e`) would end the experiment
  # and tear the deployment down after a single attempt, spending none of the retry budget.
  if ! (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" 04-kill-processes.yml
  ); then
    echo "--> WARNING: cleanup failed; retrying the run anyway." >&2
    return 0
  fi
  echo "--> Cleanup complete."
}

function build_binaries() {
  rustup target add x86_64-unknown-linux-musl
  cargo build --target x86_64-unknown-linux-musl --release
}

# TODO: put this in a separate file
get_regions() {
  local type=$1
  local size=$2
  case "$type-$size" in
    aws-random-3) echo "ca-central-1,eu-west-3,ap-northeast-2";;
    aws-random-5) echo "ca-central-1,eu-west-3,ap-southeast-7,ap-southeast-5,ap-northeast-2";;
    aws-random-7) echo "us-west-1,us-east-1,ca-central-1,eu-west-3,ap-southeast-7,ap-southeast-5,ap-northeast-2";;
    aws-random-9) echo "us-west-1,us-east-1,ca-central-1,eu-west-3,eu-west-1,eu-north-1,ap-southeast-7,ap-southeast-5,ap-northeast-2";;
    aws-random-11) echo "us-west-1,us-east-1,ca-central-1,eu-west-3,eu-west-1,eu-north-1,ap-south-1,ap-southeast-7,ap-southeast-5,ap-northeast-2,ap-northeast-1";;
    aws-random-13) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-north-1,ap-south-1,ap-southeast-7,ap-southeast-5,ap-northeast-2,ap-northeast-1";;
    aws-random-15) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-north-1,ap-south-1,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-northeast-1,ap-east-2";;
    aws-random-17) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-north-1,ap-south-1,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-northeast-1,ap-east-1,ap-east-2";;
    aws-random-19) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-1,ap-south-1,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-northeast-1,ap-east-1,ap-east-2";;
    aws-random-21) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-1,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-random-23) echo "us-west-1,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-1,il-central-1,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-random-25) echo "us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-1,il-central-1,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-random-27) echo "us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-random-29) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-random-31) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    
    aws-from-paris-3) echo "eu-west-3,eu-west-2,eu-central-1";;
    aws-from-paris-5) echo "eu-west-3,eu-west-2,eu-south-2,eu-central-2,eu-central-1";;
    aws-from-paris-7) echo "eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-central-2,eu-central-1";;
    aws-from-paris-9) echo "eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1";;
    aws-from-paris-11) echo "us-east-1,ca-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1";;
    aws-from-paris-13) echo "us-east-2,us-east-1,ca-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-1";;
    aws-from-paris-15) echo "us-east-2,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1";;
    aws-from-paris-17) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1";;
    aws-from-paris-19) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,af-south-1";;
    aws-from-paris-21) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-5,af-south-1";;
    aws-from-paris-23) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,af-south-1";;
    aws-from-paris-25) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-east-1,af-south-1";;
    aws-from-paris-27) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-from-paris-29) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    aws-from-paris-31) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,sa-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,ap-east-2,af-south-1";;
    *) echo "";;
  esac
}

# Experiment 1: end-to-end latency. Feeds Figure 1 (Introduction) and Figure 7
# (End-to-End Latency) -- one set of runs, two figures in two different sections.
function exp-1() {
  echo "--- Starting Experiment 1: Pure Latency ---"
  for configName in "${EXP1_CONFIGS[@]}"; do
    local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-1-$configName")"
    local varFile="${CONFIGS[$configName]}"
    provision "$varFile" "$EXPERIMENT_ID"
    deploy "$EXPERIMENT_ID"

    for writes in 1; do
      for algo in "${ALGOS[@]}"; do
        run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$DURATION" "exponential" "$THROUGHPUT"
      done
    done

    destroy "$varFile" "$EXPERIMENT_ID"
  done

  echo "--- Finished Experiment 1 ---"
}

# Legacy: latency under conflicts. Maps to no figure in the current paper; kept as the
# starting point for the camera-ready conflicts experiment. Not part of `all`.
function exp-conflicts() {
  echo "--- Starting Legacy Conflicts Experiment: Latency under load ---"

  local configName="aws-ring-7"
  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-conflicts")"
  local varFile="${CONFIGS[$configName]}"

  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  for writes in 1; do
    for skew in 0.5 1 2; do # 0 will have run before
      for algo in "${ALGOS[@]}"; do
        if [[ "$algo" == "kcensus" ]]; then
          # Run with conflicts
          run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$DURATION" "exponential" "$THROUGHPUT" "" "$KEYS" "$skew" "$KEYS" "true"
        fi
        # Run without conflicts
        run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$DURATION" "exponential" "$THROUGHPUT" "" "$KEYS" "$skew" "$KEYS" "false"
      done
    done
  done

  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--- Finished Legacy Conflicts Experiment ---"
}

# Experiment 2: impact of failures on latency. Feeds Figure 8.
function exp-2() {
  echo "--- Starting Experiment 2: Faults ---"

  local configName="aws-ring-7"
  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-2")"
  local varFile="${CONFIGS[$configName]}"
  local writes=1
  local duration="10s"
  local throughput="$THROUGHPUT"

  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  for algo in "${REPLICATED_ALGOS[@]}"; do
    for faults in "" $(all_faults "$(digits "$configName")" "$(get_nonvoting "$configName" "$algo")"); do
      run "$EXPERIMENT_ID" "$configName" "$algo" $writes $duration exponential $throughput "$faults"
    done
  done

  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--- Finished Experiment 2 ---"
}

function exp-3() {
  echo "--- Starting Experiment 3: Scalability and Propagation ---"

  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-3")"
  local varFile="deployment/terraform/regions/aws-31.tfvars"
  local tmpDir="$(pwd)/.tmp_configs_${EXPERIMENT_ID}"
  local masterConfigFile="${tmpDir}/master-config.json"
  local duration="10s"

  # step 1: provision servers
  provision "$varFile" "$EXPERIMENT_ID"
  mkdir -p "$tmpDir"

  # step 2: prepare nodes and generate master config file
  echo "--> Preparing nodes and generating master config file"
  (
    cd deployment/ansible
    ansible-playbook -i "inventory-${EXPERIMENT_ID}.ini" 01-prepare-nodes.yml
    ansible-playbook -i "inventory-${EXPERIMENT_ID}.ini" 02-generate-configs.yml \
      -e "master_config_path=${masterConfigFile}"
  )
  echo "--> Master config created at ${masterConfigFile}"

  # step 3: run stuff
  for configs_type in aws-from-paris aws-random; do
    for num_replicas in $(seq 31 -2 3); do
      local configName="${configs_type}-${num_replicas}"
      local target_regions=$(get_regions "$configs_type" "$num_replicas")

      # step 3.1: generate sub-config file for current experiment using master config
      echo "--> Generating sub-config for ${configName} with ${num_replicas} nodes"
      local subConfigFile="${tmpDir}/config-${configName}.toml"
      local subInventoryFile="${tmpDir}/inventory-${configName}.ini"

      python3 deployment/ansible/scripts/generate_sub_config.py \
        --master-config "$masterConfigFile" \
        --regions "$target_regions" \
        --out-config "$subConfigFile" \
        --out-inventory "$subInventoryFile"

      # step 3.2: customize and distribute config files to each server (once per server set)
      echo "--> Customizing and distributing config files for ${configName}"
      (
        cd deployment/ansible
        ansible-playbook -i "${subInventoryFile}" 03-prepare-sub-configs.yml \
          -e "sub_config_file=${subConfigFile}"
      )

      # step 3.3: run the scalability experiments
      for writes in 1; do
        for algo in "${ALGOS[@]}"; do
            local configName="${configs_type}/${num_replicas}.toml" ingress=exponential
            local throughput="${THROUGHPUT}" speedup="${SPEEDUP}" faults="" conflicts=false
            local keys="${KEYS}" skew="${SKEW}" shards="${SHARDS}"
            run_title="$(make_title)"
            resultPath="${ABSOLUTE_BASE_LOG_DIR}/${run_title}"
            mkdir -p "$resultPath"

            local per_proposer_throughput; per_proposer_throughput="$(per_proposer_rate "$THROUGHPUT" "$num_replicas")"

            echo "--> RUNNING: ${run_title}"
            for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
              if (
                cd deployment/ansible
                ansible-playbook -i "${subInventoryFile}" 03-run-experiment.yml \
                  -e "algo=${algo}" -e "writes=${writes}" -e "duration=${duration}" \
                  -e "ingress=exponential" -e "throughput=${per_proposer_throughput}" -e "speedup=${SPEEDUP}" \
                  -e "keys=${KEYS}" -e "skew=${SKEW}" -e "shards=${SHARDS}" \
                  -e "result_path=${resultPath}" -e "sub_config_file=${subConfigFile}" \
                  -e "failed_path=${ABSOLUTE_BASE_LOG_DIR}/failed/${run_title}/attempt=${attempt}"
              ); then
                break
              fi
              echo "--> Attempt ${attempt}/${MAX_ATTEMPTS} failed: ${algo} on ${configName}" >&2
              if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
                echo "--> FAILED after ${MAX_ATTEMPTS} attempts: ${run_title}" >&2
                exit 1
              fi
              cleanup_processes "${EXPERIMENT_ID}"
              retry_backoff "${attempt}"
            done
        done
      done

      # step 3.4: run one propagation experiment per config
      local graph_bench_title="c=${configs_type}/${num_replicas}.toml"
      local graphResultPath="${ABSOLUTE_BASE_LOG_DIR}/${graph_bench_title}"
      mkdir -p "$graphResultPath"

      echo "--> RUNNING Graph Bench: ${graph_bench_title}"
      (
        cd deployment/ansible
        ansible-playbook -i "${subInventoryFile}" 03-run-graph-bench.yml \
          -e "result_path=${graphResultPath}"
      )
    done
  done

  # step 4: destroy infrastructure
  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--- Finished Experiment 3 ---"
}

function exp-4() {
  echo "--- Starting Experiment 4: Resources ---"

  local configName="aws-exp-4"
  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-4")"
  local varFile="${CONFIGS[$configName]}"
  local inventoryFile="inventory-${EXPERIMENT_ID}.ini"

  provision "$varFile" "$EXPERIMENT_ID"

  (
    cd deployment/ansible/
    ansible-playbook -i "${inventoryFile}" exp-4-resources.yml
  )

  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--> Processing and merging experiment results..."
  local archive="${ABSOLUTE_BASE_LOG_DIR}/exp-4-resources_logs.tar.gz"
  echo "--> Extracting and merging $archive..."
  tar -xzf "$archive" -C "${ABSOLUTE_BASE_LOG_DIR}" --strip-components=1
  echo "--> Results successfully merged into ${ABSOLUTE_BASE_LOG_DIR}"

  echo "--- Finished Experiment 4 ---"
}

function init_environment() {
  echo "Initializing environment..."
  mkdir -p "${BASE_LOG_DIR}"
  ABSOLUTE_BASE_LOG_DIR="$(cd "${BASE_LOG_DIR}" && pwd)"
  activate_env
}

function run_all_experiments() {
  echo "Running All Experiments"
  exp-1
  exp-2
  exp-3
  exp-4
  # exp-conflicts is deliberately excluded: it maps to no figure in the current paper.
}

function main() {
  if [[ $# -eq 0 ]]; then
    show_help
    exit 0
  fi

  init_environment
  build_binaries

  local command="$1"
  shift

  case "$command" in
    "exp-1")
      exp-1
      ;;
    "exp-2")
      exp-2
      ;;
    "exp-3")
      exp-3
      ;;
    "exp-4")
      exp-4
      ;;
    "exp-conflicts")
      exp-conflicts
      ;;
    "all")
      run_all_experiments
      ;;
    "destroy")
      if [[ $# -ne 2 ]]; then
        echo "Usage: $0 destroy <terraform-var-file> <experiment-id>"
        exit 1
      fi
      destroy "$1" "$2"
      ;;
    "help"|"-h"|"--help")
      show_help
      ;;
    *)
      echo "Error: Unknown command '$command'"
      echo ""
      show_help
      exit 1
      ;;
  esac
}

trap teardown_on_abort EXIT INT TERM

main "$@"
