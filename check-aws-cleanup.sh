#!/usr/bin/env bash

# Verify that KCensus instances have all been terminated.

set -u

RUN_ID=""

function usage() {
  cat <<'EOF'
Usage: ./check-aws-cleanup.sh [--run-id ID]

Checks enabled AWS regions for non-terminated KCensus instances. With --run-id, checks only
instances belonging to that ID; otherwise, checks every KCensus instance in the account.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --run-id)
      [ $# -ge 2 ] || { echo "Error: --run-id requires a value." >&2; exit 2; }
      RUN_ID="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Error: unknown argument '$1'." >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [ -n "$RUN_ID" ] && [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]]; then
  echo "Error: --run-id must be 1-32 lowercase letters, digits, or hyphens, and start with a letter or digit." >&2
  exit 2
fi

export AWS_RETRY_MODE=standard
export AWS_MAX_ATTEMPTS=2
export AWS_PAGER=""

if ! regions=$(aws ec2 describe-regions \
    --region us-east-1 \
    --query 'Regions[].RegionName' \
    --output text \
    --cli-connect-timeout 5 \
    --cli-read-timeout 20); then
  echo "ERROR: Could not list enabled AWS regions." >&2
  exit 2
fi

found=0
errors=0

filters=(
  'Name=tag:Name,Values=kcensus-*'
  'Name=instance-state-name,Values=pending,running,shutting-down,stopping,stopped'
)

if [ -n "$RUN_ID" ]; then
  # Use exact experiment IDs rather than a suffix wildcard, which could make a short run ID such
  # as "1" also match another reviewer's "reviewer-1".
  experiment_ids=(
    "exp-1-aws-ring-7-${RUN_ID}"
    "exp-1-aws-europe-7-${RUN_ID}"
    "exp-1-aws-north-america-7-${RUN_ID}"
    "exp-1-aws-east-asia-7-${RUN_ID}"
    "exp-2-${RUN_ID}"
    "exp-3-${RUN_ID}"
    "exp-4-${RUN_ID}"
    "exp-conflicts-${RUN_ID}"
  )
  experiment_id_filter=$(IFS=,; echo "${experiment_ids[*]}")
  filters+=("Name=tag:ExperimentID,Values=${experiment_id_filter}")
fi

for region in $regions; do
  # KCensus has no AMI or Terraform deployment in these regions.
  case "$region" in
    me-south-1|me-central-1) continue ;;
  esac

  if ! instances=$(aws ec2 describe-instances \
      --region "$region" \
      --filters "${filters[@]}" \
      --query "Reservations[].Instances[].[InstanceId,State.Name,Tags[?Key=='Name']|[0].Value]" \
      --output text \
      --cli-connect-timeout 5 \
      --cli-read-timeout 20); then
    echo "WARNING: Could not inspect $region." >&2
    errors=1
    continue
  fi

  if [ -n "$instances" ]; then
    found=1
    echo "$region"
    while IFS= read -r instance; do
      echo "  $instance"
    done <<< "$instances"
  fi
done

if [ "$errors" -ne 0 ]; then
  echo "Cleanup could not be verified because at least one region could not be inspected." >&2
  exit 2
fi
if [ "$found" -ne 0 ]; then
  if [ -n "$RUN_ID" ]; then
    echo "Non-terminated instances remain for run ID '$RUN_ID'." >&2
  else
    echo "Non-terminated KCensus instances remain." >&2
  fi
  exit 1
fi

if [ -n "$RUN_ID" ]; then
  echo "Cleanup verified: no non-terminated instances remain for run ID '$RUN_ID'."
else
  echo "Cleanup verified: no non-terminated KCensus instances remain."
fi
