#!/usr/bin/env bash

# Verify that KCensus instances have all been terminated, and optionally terminate them.

set -u

RUN_ID=""
TERMINATE=0
TERMINATE_ID=""

function usage() {
  cat <<'EOF'
Usage: ./check-aws-cleanup.sh [--run-id ID] [--terminate [INSTANCE_ID]]

Checks enabled AWS regions for non-terminated KCensus instances. With --run-id, checks only
instances belonging to that ID; otherwise, checks every KCensus instance in the account.

  --terminate [INSTANCE_ID]
               Terminate what is found, instead of only reporting it. Use after an interrupted
               `terraform apply`, whose instances Terraform may not have recorded in its state:
               those keep their security group alive, so a later `destroy` hangs on it until it
               times out. Terminating them lets the destroy through.

               Given an instance id, terminates only that one -- every region is still scanned,
               to find where it lives, and everything found is still listed.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --run-id)
      [ $# -ge 2 ] || { echo "Error: --run-id requires a value." >&2; exit 2; }
      RUN_ID="$2"
      shift 2
      ;;
    --terminate)
      TERMINATE=1
      # An instance id may follow, to terminate only that one. Another flag, or nothing, leaves
      # every instance found targeted.
      if [ $# -ge 2 ] && [[ "$2" =~ ^i-[0-9a-f]+$ ]]; then
        TERMINATE_ID="$2"
        shift
      fi
      shift
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
terminated=0

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
  # Unreachable at the time of writing, so every call to them hangs until it times out. No
  # deployment targets them either, so there is nothing to find.
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

    if [ "$TERMINATE" -eq 1 ]; then
      ids=$(echo "$instances" | awk '{print $1}')
      if [ -n "$TERMINATE_ID" ]; then
        ids=$(echo "$ids" | grep -Fx "$TERMINATE_ID" || true)
      fi
    fi
    if [ "$TERMINATE" -eq 1 ] && [ -n "$ids" ]; then
      # shellcheck disable=SC2086 -- the ids are a deliberate argument list.
      if aws ec2 terminate-instances \
          --region "$region" \
          --instance-ids $ids \
          --query 'TerminatingInstances[].InstanceId' \
          --output text \
          --cli-connect-timeout 5 \
          --cli-read-timeout 20 >/dev/null; then
        terminated=1
        echo "  -> termination requested"
      else
        echo "WARNING: Could not terminate instances in $region." >&2
        errors=1
      fi
    fi
  fi
done

if [ "$errors" -ne 0 ]; then
  echo "Cleanup could not be verified because at least one region could not be inspected." >&2
  exit 2
fi
if [ -n "$TERMINATE_ID" ] && [ "$terminated" -eq 0 ]; then
  echo "Error: $TERMINATE_ID was not found in any region." >&2
  exit 1
fi
if [ "$found" -ne 0 ]; then
  if [ "$terminated" -ne 0 ]; then
    echo "Re-run until nothing is listed before destroying: a security group is only released"
    echo "once its instances reach 'terminated', not 'shutting-down'."
    exit 0
  fi
  if [ -n "$RUN_ID" ]; then
    echo "Non-terminated instances remain for run ID '$RUN_ID'." >&2
  else
    echo "Non-terminated KCensus instances remain." >&2
  fi
  cat >&2 <<'HINT'

Tear them down with:
  ./eval.sh destroy <terraform-var-file> <experiment-id>

If that hangs on a security group, some of these are not in Terraform's state -- an interrupted
apply leaves instances it never recorded, and they hold the group open. Terminate them directly,
then let the destroy finish:
  ./check-aws-cleanup.sh --terminate <instance-id>   # the i-xxx above; omit it to take all
HINT
  exit 1
fi

if [ -n "$RUN_ID" ]; then
  echo "Cleanup verified: no non-terminated instances remain for run ID '$RUN_ID'."
else
  echo "Cleanup verified: no non-terminated KCensus instances remain."
fi
