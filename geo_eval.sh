#!/usr/bin/env bash

set -e

# Infrastructure currently provisioned. An abort (exhausted retries, `set -e`, or Ctrl-C) must
# still tear it down: `run` aborting with `exit` would otherwise skip the `destroy` at the end of
# the calling experiment and leave 7-31 instances billing.
CURRENT_VAR_FILE=""
CURRENT_EXP_ID=""

# Bounded retries for a failed experiment run. A failure here is rare -- a kcensus panic (e.g.
# experiment timeout), the 60s timeout, or a transient SSH/node problem -- so exhausting all
# attempts means something is genuinely wrong and the script aborts rather than looping forever.
# The delay is applied *before* the retry to give transient causes time to clear. Process cleanup
# itself is a barrier: it waits for kcensus to exit and for port 8000 to stop listening.
# MAX_ATTEMPTS comes from lib.sh; the delays are AWS-only (a local retry has nothing to wait for).
RETRY_DELAYS=(2 60 120 300)   # before attempts 2, 3, 4 and 5 respectively

# Experiment definitions shared with eval.sh (algorithms, workload, configs, log-path schema).
source "$(dirname "$0")/lib.sh"

BASE_LOG_DIR="./logs"

declare -A CONFIGS
CONFIGS["aws-ring-7"]="deployment/terraform/regions/ring-7.tfvars"
CONFIGS["aws-north-america-7"]="deployment/terraform/regions/north-america-7.tfvars"
CONFIGS["aws-europe-7"]="deployment/terraform/regions/europe-7.tfvars"
CONFIGS["aws-east-asia-7"]="deployment/terraform/regions/east-asia-7.tfvars"
CONFIGS["aws-exp-4"]="deployment/terraform/regions/one.tfvars"

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

# Tear down whatever is still provisioned when the script exits. `destroy` clears both
# variables, so a non-empty `CURRENT_EXP_ID` here means infrastructure is still up.
function teardown_on_abort() {
  local code=$?
  trap - EXIT INT TERM
  if [ -n "${CURRENT_EXP_ID}" ]; then
    echo "--> Aborting: tearing down ${CURRENT_EXP_ID} before exit..." >&2
    destroy "${CURRENT_VAR_FILE}" "${CURRENT_EXP_ID}" || echo \
      "--> WARNING: teardown FAILED. Destroy manually:" \
      "./geo_eval.sh destroy ${CURRENT_VAR_FILE} ${CURRENT_EXP_ID}" >&2
    # A zero status with infrastructure still up means a signal: bash defers the trap until
    # the running command returns, so `$?` is that command's success.
    [ "${code}" -eq 0 ] && code=130
  fi
  exit "${code}"
}

# Check a completed attempt after Ansible has collected all process logs. This detects occasional
# significant network fluctuations that would unfairly distort an individual data point. Such a
# quality failure is retryable, except on the final attempt: keeping that result is preferable to
# failing an entire deployment after exhausting the available samples. Checker errors are never
# ignored.
# Exit codes of graphs/check_expected_latency.py.
CHECK_LATENCY_VIOLATION=1
CHECK_STALL=3
# A run the quality checker rejects is worth more attempts than the default: whatever it
# caught -- a stall, or latency simply above what the deployment should produce -- says the
# measurement was disturbed rather than that the rung is past capacity, so another attempt has
# a real chance of coming back clean. `run` raises its budget to this, keeping `MAX_ATTEMPTS`
# when that is already larger. A run killed by its own deadline earns the same only when its
# partial logs show a stall; otherwise it really is over capacity and retrying will not help.
QUALITY_MAX_ATTEMPTS=4
# Set by `latency_quality_passes` and `run_stalled` for the retry loop to read.
LAST_RUN_STALLED=0
# Where `run` put the logs of the run it just completed.
LAST_RUN_RESULT_PATH=""
# Whether the run `exp-5-run` just judged was rejected for its latency alone.
LAST_RUN_OVER_LATENCY_CAP=0
# The average latency, in ms, of the run `exp-5-run` just judged. Empty when it has none.
LAST_RUN_AVERAGE=""

function latency_quality_passes() {
  local resultPath="$1"
  local failedPath="$2"
  local checkerStatus=0

  python3 "$(dirname "$0")/graphs/check_expected_latency.py" "${resultPath}" || checkerStatus=$?
  LAST_RUN_STALLED=0
  if [ "${checkerStatus}" -eq "${CHECK_STALL}" ]; then
    LAST_RUN_STALLED=1
  fi
  if [ "${checkerStatus}" -eq 0 ]; then
    return 0
  fi
  if [ "${checkerStatus}" -ne "${CHECK_LATENCY_VIOLATION}" ] \
      && [ "${checkerStatus}" -ne "${CHECK_STALL}" ]; then
    echo "--> ERROR: latency quality checker exited ${checkerStatus}." >&2
    return "${checkerStatus}"
  fi

  if ! mkdir -p "${failedPath}" || ! cp -a "${resultPath}/." "${failedPath}/"; then
    echo "--> ERROR: could not preserve latency-rejected output in ${failedPath}." >&2
    return 2
  fi
  echo "--> Latency quality checks failed; output kept in ${failedPath}." >&2
  return 1
}

# Whether the partial logs of a run that never finished show a stall.
#
# A run killed by its own deadline still wrote everything it executed before it stalled, so the
# profile is there to read. Without this a stall that pushed a rung over its deadline would be
# indistinguishable from that rung simply being past the deployment's capacity.
function run_stalled() {
  local resultPath="$1" checkerStatus=0
  python3 "$(dirname "$0")/graphs/check_expected_latency.py" "${resultPath}" \
    > /dev/null 2>&1 || checkerStatus=$?
  LAST_RUN_STALLED=0
  if [ "${checkerStatus}" -eq "${CHECK_STALL}" ]; then
    LAST_RUN_STALLED=1
  fi
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

  local inventoryFile="inventory-${expId}.ini"
  local title; title="$(make_title)"
  local resultPath="${ABSOLUTE_BASE_LOG_DIR}/${title}"
  mkdir -p "${resultPath}"
  LAST_RUN_RESULT_PATH="${resultPath}"

  echo "--> RUNNING: ${title} (Exp ID: ${expId})"

  local proposer_count="$(digits "$configName")"
  local per_proposer_throughput; per_proposer_throughput="$(per_proposer_rate "$throughput" "$proposer_count")"

  # The budget starts at `MAX_ATTEMPTS` and is raised the first time a run looks disturbed
  # rather than over capacity, so such a run gets `max(MAX_ATTEMPTS, QUALITY_MAX_ATTEMPTS)`
  # chances to come back clean.
  local attempt=1 limit="${MAX_ATTEMPTS}"
  while [ "${attempt}" -le "${limit}" ]; do
    local failedPath="${ABSOLUTE_BASE_LOG_DIR}/failed/${title}/attempt=${attempt}"
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
        -e "conflicts=${conflicts}" \
        -e "result_path=${resultPath}" \
        -e "failed_path=${failedPath}"
    ); then
      if latency_quality_passes "${resultPath}" "${failedPath}"; then
        break
      fi
      local quality_failure=1 disturbed=1
      local disturbed_reason="Failed the quality checks"
    else
      # The run never finished -- its own deadline, or a crash. It still logged everything it
      # executed first, so a stall that caused the deadline is visible in what is there.
      run_stalled "${resultPath}"
      local quality_failure=0 disturbed="${LAST_RUN_STALLED}"
      local disturbed_reason="Stalled mid-run rather than saturated"
    fi

    if [ "${disturbed}" -eq 1 ] && [ "${limit}" -lt "${QUALITY_MAX_ATTEMPTS}" ]; then
      limit="${QUALITY_MAX_ATTEMPTS}"
      echo "--> ${disturbed_reason}; allowing up to ${limit} attempts." >&2
    fi

    echo "--> Attempt ${attempt}/${limit} failed: ${algo} on ${configName} (faults=${faults})" >&2
    if [ "${attempt}" -ge "${limit}" ]; then
      # A quality failure on the last attempt is kept rather than discarded: it is a measured
      # run, only a worse one than expected. A run that never finished is a failure outright.
      if [ "${quality_failure}" -eq 1 ]; then
        echo "--> WARNING: final attempt failed the quality checks; keeping it anyway." >&2
        break
      fi
      echo "--> FAILED after ${limit} attempts: ${title}" >&2
      return 1
    fi
    # A crashed run can leave a kcensus process alive. Cleanup does not return until all such
    # processes are gone and port 8000 is no longer listening.
    cleanup_processes "${expId}"
    retry_backoff "${attempt}"
    attempt=$((attempt + 1))
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
  # Do not start another attempt unless every node confirms that kcensus is gone. Otherwise a
  # cleanup/SSH failure can turn one bad attempt into a cross-run connection mix-up.
  if ! (
    cd deployment/ansible
    ansible-playbook -i "${inventoryFile}" 04-kill-processes.yml
  ); then
    echo "--> ERROR: cleanup could not be verified; refusing to start another attempt." >&2
    return 1
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
        run "$EXPERIMENT_ID" "$configName" "$algo" "$writes" "$BASELINE_DURATION" "exponential" "$THROUGHPUT"
      done
    done

    destroy "$varFile" "$EXPERIMENT_ID"
  done

  echo "--- Finished Experiment 1 ---"
}

# Experiment 5: contention and load. Feeds Figures 13 and 14.
function exp-5() {
  echo "--- Starting Experiment 5: Conflicts and load ---"

  local configName="${EXP5_CONFIG}"
  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-5")"
  # The ladder is the only experiment whose offered rate can outruns t3.medium's CPU-credit
  # baseline, so it takes the same region set on a non-burstable instance type.
  local varFile="deployment/terraform/regions/ring-7-load.tfvars"

  # The ladder walks past what the deployment sustains, so failures at the top are expected
  # and a rung is only believed unsustainable once every attempt has failed.
  # `local` is dynamically scoped in bash, so `run` sees this.
  local MAX_ATTEMPTS=2

  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  local algo skew
  # The CDF point: one rate, every skew, at the longer baseline duration.
  for skew in "${EXP5_SKEWS[@]}"; do
    for algo in "${ALGOS[@]}"; do
      exp-5-run "$EXPERIMENT_ID" "$configName" "$algo" "$EXP5_CDF_THROUGHPUT" "$skew" \
        "$BASELINE_DURATION"
    done
  done

  # One series per line of the load figure: every algorithm at every load skew, and
  # multi-paxos once more with pipelining.
  local series=()
  for skew in "${EXP5_LOAD_SKEWS[@]}"; do
    for algo in "${ALGOS[@]}"; do
      series+=("${algo}|${skew}|true")
    done
  done
  series+=("multi-paxos|0|false")
  exp-5-ladder "$EXPERIMENT_ID" "$configName" "${series[@]}"

  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--- Finished Experiment 5 ---"
}

# Runs one `algo|skew|conflicts` series at one offered rate.
function exp-5-series-run() {
  local expId="$1" configName="$2" entry="$3" throughput="$4"
  local algo skew conflicts
  IFS='|' read -r algo skew conflicts <<< "$entry"
  exp-5-run "$expId" "$configName" "$algo" "$throughput" "$skew" "$DURATION" "$conflicts"
}

# Walks the load ladder for every series at once.
#
# Runs are ordered by offered rate rather than by series: the figure compares algorithms at a
# rate, and a sweep this long outlives the network conditions it started in, so two series
# measured at one rate are measured minutes apart rather than hours.
#
# Phase 1 doubles. Doubling finds the wall in a logarithmic number of runs wherever it is,
# which is what lets one ladder definition serve algorithms whose walls are two decades apart.
#
# A rate that fails is retried once, at the same rate, in the next round -- a rate lost to a
# slow instance or a retried deployment would otherwise anchor the refinement a full factor of
# two below the real wall, and the retry lands after the rest of the deployment has moved on.
# The retry comes first in that round, so the series is back at the shared rate before the
# round ends and stays comparable with the others. Failing the same rate twice ends the series.
function exp-5-ladder() {
  local expId="$1" configName="$2"; shift 2
  local active=("$@") survivors=()
  local -A sustained=() pending=()
  local -A EXP5_OUTCOMES=() EXP5_AVERAGES=()
  local entry throughput="$EXP5_LADDER_START"

  while [ "$throughput" -le "$EXP5_LADDER_END" ] && [ "${#active[@]}" -gt 0 ]; do
    survivors=()
    for entry in "${active[@]}"; do
      if [ -n "${pending[$entry]:-}" ]; then
        if ! exp-5-attempt "$expId" "$configName" "$entry" "${pending[$entry]}"; then
          continue
        fi
        sustained["$entry"]="${pending[$entry]}"
        unset "pending[$entry]"
      fi
      if exp-5-attempt "$expId" "$configName" "$entry" "$throughput"; then
        sustained["$entry"]="$throughput"
      else
        pending["$entry"]="$throughput"
      fi
      survivors+=("$entry")
    done
    active=("${survivors[@]}")
    throughput=$((throughput * 2))
  done

  # Phase 2 takes every series at once, each starting from the rate it reached, so that two
  # series still measure minutes rather than hours apart once their rates have diverged.
  local pairs=()
  for entry in "$@"; do
    if [ -n "${sustained[$entry]:-}" ]; then
      pairs+=("$entry" "${sustained[$entry]}")
    fi
  done
  exp-5-refine "$expId" "$configName" "${pairs[@]}"
}

# One run, and the verdict on it.
#
# Two verdicts are final and replay without running anything again: a rate the series sustained,
# and one it measured cleanly but too slowly. Only a rate whose *run* failed is tried again,
# every caller that comes back to one being there to give it a second chance -- a rate already
# measured is not in want of one, and re-running it would only buy the same answer.
function exp-5-attempt() {
  local expId="$1" configName="$2" entry="$3" rung="$4"
  local key="${entry}@${rung}"
  case "${EXP5_OUTCOMES[$key]:-}" in
    sustained) return 0 ;;
    over-cap) return 1 ;;
  esac
  local status=0
  exp-5-series-run "$expId" "$configName" "$entry" "$rung" || status=$?
  if [ -n "${LAST_RUN_AVERAGE}" ]; then
    EXP5_AVERAGES["$key"]="${LAST_RUN_AVERAGE}"
  fi
  if [ "$status" -eq 0 ]; then
    EXP5_OUTCOMES["$key"]=sustained
    return 0
  fi
  if [ "${LAST_RUN_OVER_LATENCY_CAP}" -eq 1 ]; then
    EXP5_OUTCOMES["$key"]=over-cap
  fi
  return 1
}

# Narrows the wall each series ran into.
#
# Phase 1 leaves a series with a rate it sustained knowing that twice that rate failed, so the
# step opens at half the sustained rate and the walk adds it. A step that is still wide -- over
# `EXP5_STEP_COARSE`, or over an eighth of the rate reached and over `EXP5_STEP_FLOOR` -- is
# halved after every rung, so the walk closes in whether the wall is just above the sustained
# rate or nearly at twice it. A rate that fails without narrowing the step is tried once more
# before it is believed, one failure being as likely to be a slow instance as a real wall, so a
# series ends on a rate that failed twice with the step already as fine as it goes. That pins
# the wall to within one step.
#
# Series advance one rung per round rather than one series at a time, so two of them are still
# measured minutes rather than hours apart once their rates have diverged.
function exp-5-refine() {
  local expId="$1" configName="$2"; shift 2
  local -A sustained=() step=() missed=()
  local -a series=() active=() survivors=()
  local entry rung successful reduce_step below

  while [ "$#" -gt 0 ]; do
    series+=("$1")
    sustained["$1"]="$2"
    step["$1"]=$(( $2 / 2 ))
    missed["$1"]=0
    shift 2
  done
  active=("${series[@]}")

  while [ "${#active[@]}" -gt 0 ]; do
    survivors=()
    for entry in "${active[@]}"; do
      rung=$(( sustained[$entry] + step[$entry] ))
      if [ "$rung" -gt "$EXP5_LADDER_END" ]; then
        continue
      fi
      successful=0
      if exp-5-attempt "$expId" "$configName" "$entry" "$rung"; then
        successful=1
        sustained["$entry"]="$rung"
        missed["$entry"]=0
      fi
      reduce_step=0
      if [ "${step[$entry]}" -gt "$EXP5_STEP_COARSE" ] \
        || { [ "${step[$entry]}" -gt $(( sustained[$entry] / 8 )) ] \
          && [ "${step[$entry]}" -gt "$EXP5_STEP_FLOOR" ]; }; then
        reduce_step=1
        step["$entry"]=$(( step[$entry] / 2 ))
      fi
      if [ "$reduce_step" -eq 1 ] || [ "$successful" -eq 1 ]; then
        survivors+=("$entry")
      elif [ "${missed[$entry]}" -eq 0 ]; then
        missed["$entry"]=1
        survivors+=("$entry")
      fi
    done
    active=("${survivors[@]}")
  done

  # Three rungs of the final step below where each series stopped, so the curve has points
  # either side of the wall at the resolution the wall was found at. These sit under a rate the
  # series sustained, so a failure is a disturbed run rather than a rate out of reach, and is
  # worth the same second attempt the walk gives one.
  for below in 3 2 1; do
    for entry in "${series[@]}"; do
      rung=$(( sustained[$entry] - below * step[$entry] ))
      if [ "$rung" -gt 0 ]; then
        exp-5-attempt "$expId" "$configName" "$entry" "$rung" \
          || exp-5-attempt "$expId" "$configName" "$entry" "$rung" \
          || true
      fi
    done
  done

  # Below those, each series keeps stepping down until a rung's average latency is within
  # `EXP5_KNEE_LATENCY_FACTOR` of its average at `EXP5_LADDER_START`, so the curve shows where
  # latency starts to rise. The step doubles at every rung, and each rung is the largest multiple
  # of the step below the previous one, which lands on the rates phase 1 already measured. The
  # step never exceeds half the previous rung.
  local -A position=() back=()
  active=()
  for entry in "${series[@]}"; do
    position["$entry"]=$(( sustained[$entry] - 3 * step[$entry] ))
    back["$entry"]="${step[$entry]}"
    if ! exp-5-below-knee "$entry" "${position[$entry]}"; then
      active+=("$entry")
    fi
  done
  while [ "${#active[@]}" -gt 0 ]; do
    survivors=()
    for entry in "${active[@]}"; do
      back["$entry"]=$(( back[$entry] * 2 ))
      while [ $(( back[$entry] * 2 )) -gt "${position[$entry]}" ] && [ "${back[$entry]}" -gt "$EXP5_LADDER_START" ]; do
        back["$entry"]=$(( back[$entry] / 2 ))
      done
      rung=$(( (position[$entry] - 1) / back[$entry] * back[$entry] ))
      position["$entry"]="$rung"
      if [ "$rung" -lt "$EXP5_LADDER_START" ]; then
        continue
      fi
      exp-5-attempt "$expId" "$configName" "$entry" "$rung" \
        || exp-5-attempt "$expId" "$configName" "$entry" "$rung" \
        || true
      if ! exp-5-below-knee "$entry" "$rung"; then
        survivors+=("$entry")
      fi
    done
    active=("${survivors[@]}")
  done
}

# Whether a series measured `rung` with an average latency under `EXP5_KNEE_LATENCY_FACTOR`
# times its average at `EXP5_LADDER_START`. False when either average is missing.
function exp-5-below-knee() {
  local entry="$1" rung="$2"
  local base="${EXP5_AVERAGES[${entry}@${EXP5_LADDER_START}]:-}"
  local average="${EXP5_AVERAGES[${entry}@${rung}]:-}"
  [ -n "$base" ] && [ -n "$average" ] \
    && awk -v avg="$average" -v base="$base" -v factor="$EXP5_KNEE_LATENCY_FACTOR" \
      'BEGIN { exit !(avg < factor * base) }'
}

# One rung of the ladder. Not sustained if the run did not finish, and not sustained either if
# it finished slowly: past the knee a deployment still answers every request, just far later
# than it did a rung earlier, so latency is what says the wall has been reached rather than
# whether the run survived. A run whose latency cannot be read is not sustained either, and
# unlike a slow one it is left retryable -- nothing was measured, so nothing was learnt.
function exp-5-run() {
  local expId="$1" configName="$2" algo="$3" throughput="$4" skew="$5" duration="$6"
  local conflicts="${7:-true}"
  local keys="$KEYS"
  LAST_RUN_OVER_LATENCY_CAP=0
  LAST_RUN_AVERAGE=""
  if ! run "$expId" "$configName" "$algo" "$EXP5_WRITES" "$duration" "exponential" \
         "$throughput" "" "$keys" "$skew" "$keys" "$conflicts"; then
    echo "--> SKIPPED (not sustainable?): ${algo} t=${throughput} skew=${skew} k=${keys}" \
      "conflicts=${conflicts}" >&2
    return 1
  fi

  local average
  if ! average="$(python3 "$(dirname "$0")/graphs/run_average_latency.py" \
      "${LAST_RUN_RESULT_PATH}")"; then
    echo "--> NOT SUSTAINED (no average latency in ${LAST_RUN_RESULT_PATH}):" \
      "${algo} t=${throughput} skew=${skew} k=${keys} conflicts=${conflicts}" >&2
    return 1
  fi
  LAST_RUN_AVERAGE="${average}"
  if awk -v avg="${average}" -v max="${EXP5_SUSTAINED_MAX_LATENCY_MS}" \
      'BEGIN { exit !(avg > max) }'; then
    echo "--> NOT SUSTAINED (average ${average} ms, over ${EXP5_SUSTAINED_MAX_LATENCY_MS} ms):" \
      "${algo} t=${throughput} skew=${skew} k=${keys} conflicts=${conflicts}" >&2
    LAST_RUN_OVER_LATENCY_CAP=1
    return 1
  fi
  echo "--> sustained at ${average} ms average"
}

# Experiment 2: impact of failures on latency. Feeds Figure 8.
function exp-2() {
  echo "--- Starting Experiment 2: Faults ---"

  local configName="aws-ring-7"
  local EXPERIMENT_ID; EXPERIMENT_ID="$(experiment_id "exp-2")"
  local varFile="${CONFIGS[$configName]}"
  local writes=1
  local throughput="$THROUGHPUT"

  provision "$varFile" "$EXPERIMENT_ID"
  deploy "$EXPERIMENT_ID"

  local algo duration faults
  # Iterate over algos last so network conditions are as similar as possible.
  for faults in "" $(all_faults "$(digits "$configName")"); do
    if [ -z "$faults" ]; then duration="$BASELINE_DURATION"; else duration="$DURATION"; fi
    for algo in "${REPLICATED_ALGOS[@]}"; do
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
              local failedPath="${ABSOLUTE_BASE_LOG_DIR}/failed/${run_title}/attempt=${attempt}"
              if (
                cd deployment/ansible
                ansible-playbook -i "${subInventoryFile}" 03-run-experiment.yml \
                  -e "algo=${algo}" -e "writes=${writes}" -e "duration=${duration}" \
                  -e "ingress=exponential" -e "throughput=${per_proposer_throughput}" -e "speedup=${SPEEDUP}" \
                  -e "keys=${KEYS}" -e "skew=${SKEW}" -e "shards=${SHARDS}" \
                  -e "result_path=${resultPath}" -e "sub_config_file=${subConfigFile}" \
                  -e "failed_path=${failedPath}"
              ); then
                if latency_quality_passes "${resultPath}" "${failedPath}" "${attempt}" "${MAX_ATTEMPTS}"; then
                  break
                fi
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
  local archive="${ABSOLUTE_BASE_LOG_DIR}/exp-4-resources_logs.tar.gz"
  local experiment_status=0

  provision "$varFile" "$EXPERIMENT_ID"

  (
    cd deployment/ansible/
    ansible-playbook -i "${inventoryFile}" exp-4-resources.yml
  ) || experiment_status=$?

  # The playbook fetches its archive before reporting an experiment failure. Tear the instance
  # down promptly, but continue locally so the diagnostic logs are not stranded in the archive.
  destroy "$varFile" "$EXPERIMENT_ID"

  echo "--> Processing and merging experiment results..."
  if [ -f "$archive" ]; then
    echo "--> Extracting and merging $archive..."
    tar -xzf "$archive" -C "${ABSOLUTE_BASE_LOG_DIR}" --strip-components=1
    rm "$archive"
    echo "--> Results successfully merged into ${ABSOLUTE_BASE_LOG_DIR}"
  else
    echo "--> ERROR: exp-4 produced no log archive at $archive" >&2
    experiment_status=1
  fi

  if [ "$experiment_status" -ne 0 ]; then
    echo "--> Experiment 4 failed; available logs were preserved in ${ABSOLUTE_BASE_LOG_DIR}" >&2
    return "$experiment_status"
  fi

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
  # exp-4 is no longer needed. We use exp-3's aws-random runs for traffic, CPU and memory measurements.
  exp-5
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
    "exp-5")
      exp-5
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
