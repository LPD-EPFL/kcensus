#!/usr/bin/env bash

set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  echo "Usage: $0 [LOG_DIR [EXP5_LOG_DIR]]"
  echo "Extract PID-0 topology snapshots into configs/ (both log roots default to logs/)."
  exit 0
fi

LOG_DIR="${1:-logs}"
EXP5_LOG_DIR="${2:-$LOG_DIR}"
CONFIG_DIR="${CONFIG_DIR:-configs}"

extract_config() {
  local log_root="$1" topology="$2" workload_pattern="$3" output="$4"
  local source="" candidate tmp merged existing_regions logged_regions

  while IFS= read -r candidate; do
    if grep -qx -- '--- BEGIN topology config.toml ---' "$candidate"; then
      source="$candidate"
      break
    fi
  done < <(find "$log_root/c=$topology" -type f -name '0.stdout' \
             -path "$workload_pattern" | sort)

  if [ -z "$source" ]; then
    echo "No PID-0 topology config found for $topology in $log_root" >&2
    return 1
  fi

  mkdir -p "$(dirname "$output")"
  tmp="$(mktemp "${output}.tmp.XXXXXX")"
  if ! awk '
    /^--- BEGIN topology config\.toml ---$/ { blocks++; inside = 1; next }
    /^--- END topology config\.toml ---$/   { inside = 0; next }
    inside                                  { print }
    END { if (blocks != 1 || inside) exit 1 }
  ' "$source" > "$tmp"; then
    rm -f "$tmp"
    echo "Invalid topology delimiters in $source" >&2
    return 1
  fi

  # Keep the human-readable suffixes already present in the checked-in region names. Runtime
  # logs intentionally contain only AWS region IDs; the ordering must still match exactly.
  if [ -f "$output" ]; then
    existing_regions="$(
      sed -n '/^regions = \[$/,/^\]$/p' "$output" |
        sed -n "s/^[[:space:]]*'\\([^ ']*\\).*/\\1/p"
    )"
    logged_regions="$(
      sed -n '/^regions = \[$/,/^\]$/p' "$tmp" |
        sed -n "s/^[[:space:]]*'\\([^ ']*\\).*/\\1/p"
    )"
    if [ "$existing_regions" != "$logged_regions" ]; then
      rm -f "$tmp"
      echo "Region ordering in $source does not match $output" >&2
      return 1
    fi

    merged="$(mktemp "${output}.tmp.XXXXXX")"
    awk '
      FNR == NR {
        if ($0 == "regions = [") regions_block = 1
        if (regions_block) regions = regions $0 ORS
        if (regions_block && $0 == "]") regions_block = 0
        next
      }
      $0 == "raw_latencies = [" { printf "%s", regions; matrix = 1 }
      matrix { print }
    ' "$output" "$tmp" > "$merged"
    rm -f "$tmp"
    tmp="$merged"
  fi

  chmod 0644 "$tmp"
  mv "$tmp" "$output"
  echo "$output <- $source"
}

# Experiments 1--3 use writes=1. Iterate over the checked-in run topologies so adding a new
# topology config automatically makes it part of the extraction.
while IFS= read -r output; do
  relative="${output#"$CONFIG_DIR/"}"
  if [[ "$relative" == */* ]]; then
    topology="$relative"
  else
    topology="${relative%.toml}"
  fi
  extract_config "$LOG_DIR" "$topology" '*/w=1/*' "$output"
done < <(
  find "$CONFIG_DIR" -type f -name '*.toml' \
    ! -path "$CONFIG_DIR/exp-5/*" \
    ! -path "$CONFIG_DIR/aws-world-33.toml" | sort
)

# Experiment 5 reused the ring topology name after measuring a new latency matrix.
extract_config "$EXP5_LOG_DIR" aws-ring-7 '*/w=0.5/*' \
  "$CONFIG_DIR/exp-5/aws-ring-7.toml"

# aws-world-33.toml is the provisioning master, not a topology run by an experiment, so it has no
# PID-0 log from which to extract it.
echo "$CONFIG_DIR/aws-world-33.toml unchanged (provisioning master)"
