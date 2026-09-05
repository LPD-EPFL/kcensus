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

# --local may appear anywhere in the arguments; everything else is passed through untouched.
LOCAL=false
ARGS=()
for arg in "$@"; do
  if [ "$arg" = "--local" ]; then LOCAL=true; else ARGS+=("$arg"); fi
done

if [ ${#ARGS[@]} -eq 0 ]; then show_help; exit 0; fi
case "${ARGS[0]}" in
  "help"|"-h"|"--help") show_help; exit 0 ;;
esac

if [ "$LOCAL" = true ]; then
  exec "${HERE}/local_eval.sh" "${ARGS[@]}"
fi
exec "${HERE}/geo_eval.sh" "${ARGS[@]}"
