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
CONFIGS["aws-exp-6"]="deployment/terraform/regions/one.tfvars"

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

function build_binaries() {
  rustup target add x86_64-unknown-linux-musl
  cargo build --target x86_64-unknown-linux-musl --release
}

get_regions() {
  local type=$1
  local size=$2
  case "$type-$size" in
    aws-random-3) echo "ap-southeast-3,ap-southeast-7,ap-east-1";;
    aws-random-5) echo "us-west-2,mx-central-1,ap-southeast-3,ap-southeast-7,ap-east-1";;
    aws-random-7) echo "us-west-2,ca-central-1,mx-central-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-east-1";;
    aws-random-9) echo "us-west-2,us-east-2,ca-central-1,mx-central-1,eu-west-3,me-central-1,ap-southeast-3,ap-southeast-7,ap-east-1";;
    aws-random-11) echo "us-west-2,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-south-2,eu-south-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-east-1";;
    aws-random-13) echo "us-west-2,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-south-2,eu-south-1,eu-central-2,me-central-1,ap-southeast-3,ap-southeast-7,ap-east-1,af-south-1";;
    aws-random-15) echo "us-west-2,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-east-1,af-south-1";;
    aws-random-17) echo "us-west-2,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-southeast-2,ap-southeast-4,ap-east-1,af-south-1";;
    aws-random-19) echo "us-west-2,us-west-1,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-southeast-2,ap-southeast-4,ap-east-1,af-south-1";;
    aws-random-21) echo "us-west-2,us-west-1,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-south-1,me-central-1,ap-southeast-3,ap-southeast-7,ap-southeast-2,ap-southeast-4,ap-east-1,af-south-1";;
    aws-random-23) echo "us-west-2,us-west-1,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-south-1,me-central-1,ap-south-2,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-southeast-2,ap-southeast-4,ap-east-1,af-south-1";;
    aws-random-25) echo "us-west-2,us-west-1,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,af-south-1";;
    aws-random-27) echo "us-west-2,us-west-1,us-east-2,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,af-south-1";;
    aws-random-29) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,af-south-1";;
    aws-random-31) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,af-south-1";;
    
    aws-from-paris-3) echo "eu-west-3,eu-west-2,eu-central-2";;
    aws-from-paris-5) echo "eu-west-3,eu-west-2,eu-west-1,eu-central-2,eu-central-1";;
    aws-from-paris-7) echo "eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-central-2,eu-central-1";;
    aws-from-paris-9) echo "eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1";;
    aws-from-paris-11) echo "us-east-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1";;
    aws-from-paris-13) echo "us-east-2,us-east-1,ca-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1";;
    aws-from-paris-15) echo "us-east-2,us-east-1,ca-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-1";;
    aws-from-paris-17) echo "us-west-2,us-east-2,us-east-1,ca-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1";;
    aws-from-paris-19) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1";;
    aws-from-paris-21) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,af-south-1";;
    aws-from-paris-23) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-5,af-south-1";;
    aws-from-paris-25) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,af-south-1";;
    aws-from-paris-27) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-1,ap-east-1,af-south-1";;
    aws-from-paris-29) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-northeast-1,ap-east-1,af-south-1";;
    aws-from-paris-31) echo "us-west-2,us-west-1,us-east-2,us-east-1,ca-west-1,ca-central-1,mx-central-1,eu-west-3,eu-west-2,eu-west-1,eu-south-2,eu-south-1,eu-north-1,eu-central-2,eu-central-1,il-central-1,me-south-1,me-central-1,ap-south-2,ap-south-1,ap-southeast-1,ap-southeast-3,ap-southeast-7,ap-southeast-5,ap-northeast-3,ap-northeast-2,ap-southeast-2,ap-southeast-4,ap-northeast-1,ap-east-1,af-south-1";;
    *) echo "";;
  esac
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

  echo "--- Finished Experiment 4 ---"
}

function exp-3-5() {
  echo "--- Starting Experiments 3 & 5: Scalability and Propagation ---"

  local EXPERIMENT_ID="exp-3-5"
  local varFile="deployment/terraform/regions/aws-all-31.tfvars"
  local tmpDir="$(pwd)/.tmp_configs_${EXPERIMENT_ID}"
  local masterConfigFile="${tmpDir}/master-config.json"

  pushd graphs >/dev/null
  source env.sh >/dev/null 2>&1
  popd >/dev/null

  # step 1: provision servers
  provision "$varFile" "$EXPERIMENT_ID"

  mkdir -p "$tmpDir"

  # step 2: copy necessary files and generate master config file
  echo "--> Preparing nodes and generating master config file"
  (
    cd deployment/ansible
    ansible-playbook -i "inventory-${EXPERIMENT_ID}.ini" deploy_and_prep_exp3-5.yml \
      -e "master_config_path=${masterConfigFile}"
  )
  echo "--> Master config created at ${masterConfigFile}"

  local requests=10
  for configs_type in aws-random aws-from-paris; do
    for num_replicas in $(seq 3 2 31); do
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
      
      echo "--> Customize and distribute configs for c=${configs_type}/${num_replicas}.toml"
      (
        cd deployment/ansible
        ansible-playbook -i "${subInventoryFile}" prepare_configs.yml \
          -e "config_path=${subConfigFile}" \
          -e "master_config_path=${masterConfigFile}"
      )

      # step 3.2: run the scalability experiments
      for writes in 1; do
        for algo in "${ALGOS[@]}"; do
            run_title="c=${configs_type}/${num_replicas}.toml/a=${algo}/w=${writes}/r=${requests}/i=round-robin/t=0/s=${SPEEDUP}/f="
            resultPath="${ABSOLUTE_BASE_LOG_DIR}/${run_title}"
            
            echo "--> RUNNING: ${run_title}"
            (
              cd deployment/ansible
              ansible-playbook -i "${subInventoryFile}" run_kcensus_sub_exp.yml \
                -e "algo=${algo}" -e "writes=${writes}" -e "requests=${requests}" \
                -e "ingress=round-robin" -e "throughput=0" -e "speedup=${SPEEDUP}" \
                -e "config_path=${subConfigFile}" \
                -e "result_path=${resultPath}" \
                -e "master_config_path=${masterConfigFile}"
            )
        done
      done

      # step 3.3: run one propagation experiment per config
      local graph_bench_title="c=${configs_type}/${num_replicas}.toml"
      local graphResultPath="${ABSOLUTE_BASE_LOG_DIR}/${graph_bench_title}"
      mkdir -p "$graphResultPath"
      
      echo "--> RUNNING Graph Bench: ${graph_bench_title}"
      (
        cd deployment/ansible
        ansible-playbook -i "${subInventoryFile}" run_graph_bench_sub_exp.yml \
          -e "config_path=${subConfigFile}" \
          -e "result_path=${graphResultPath}"
      )
    done
  done
  
  # step 4: destroy infrastructure
  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--- Finished Experiments 3 & 5 ---"
}

function exp-6() {
  echo "--- Starting Experiment 6: Resources ---"

  local configName="aws-exp-6"
  local EXPERIMENT_ID="exp-6"
  local varFile="${CONFIGS[$configName]}"
  local inventoryFile="inventory-${EXPERIMENT_ID}.ini"

  provision "$varFile" "$EXPERIMENT_ID"
  sleep 60 # sleep 1 more minute

  (
    cd deployment/ansible/
    ansible-playbook -i "${inventoryFile}" exp6.yml
  )

  destroy "$varFile" "$EXPERIMENT_ID"

  # TODO: unarchive results and merge them

  echo "--- Finished Experiment 6 ---"
}


function main() {
  echo "Starting Geo-Replicated Evaluation."

  mkdir -p "${BASE_LOG_DIR}"
  ABSOLUTE_BASE_LOG_DIR="$(cd "${BASE_LOG_DIR}" && pwd)"

}

main