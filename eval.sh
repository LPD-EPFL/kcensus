#!/usr/bin/env bash
#
# Single entry point for the experiments.
#
# The experiments, algorithms, workload and log-path schema are identical either way -- they live
# in lib.sh, which every runner shares. Only where the replicas run differs:
#
#   ./eval.sh         exp-N   -> geo_eval.sh   : provisions a real AWS deployment per experiment
#   ./eval.sh --local exp-N   -> local_eval.sh : every replica is a process on this machine,
#                                                with the wide-area link delays simulated
#
# This mirrors plot.sh, which takes the same --local flag to read the results back.

set -e

HERE="$(cd "$(dirname "$0")" && pwd)"
source "${HERE}/lib.sh"

# Global options may appear anywhere in the arguments; everything else is passed through untouched.
LOCAL=false
RUN_ID=""
ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --local)
      LOCAL=true
      shift
      ;;
    --run-id)
      if [ $# -lt 2 ]; then
        echo "Error: --run-id requires a value." >&2
        exit 1
      fi
      RUN_ID="$2"
      shift 2
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

if [ -n "$RUN_ID" ] && [[ ! "$RUN_ID" =~ ^[a-z0-9][a-z0-9-]{0,31}$ ]]; then
  echo "Error: --run-id must be 1-32 lowercase letters, digits, or hyphens, and start with a letter or digit." >&2
  exit 1
fi
if [ "$LOCAL" = true ] && [ -n "$RUN_ID" ]; then
  echo "Error: --run-id applies only to AWS runs; omit it with --local." >&2
  exit 1
fi

export KCENSUS_RUN_ID="$RUN_ID"

if [ ${#ARGS[@]} -eq 0 ]; then show_help; exit 0; fi
case "${ARGS[0]}" in
  "help"|"-h"|"--help") show_help; exit 0 ;;
esac

if [ "$LOCAL" = true ]; then
  exec "${HERE}/local_eval.sh" "${ARGS[@]}"
fi
exec "${HERE}/geo_eval.sh" "${ARGS[@]}"
