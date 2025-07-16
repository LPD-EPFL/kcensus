#!/usr/bin/env python3
import argparse
import json
import sys
import tomli_w

parser = argparse.ArgumentParser(description="Generate sub-configurations and inventories for kcensus experiments.")
parser.add_argument("--master-config", required=True, help="Path to the master JSON config file.")
parser.add_argument("--regions", required=True, help="Comma-separated list of AWS regions for the sub-experiment.")
parser.add_argument("--out-config", required=True, help="Output path for the generated TOML config file.")
parser.add_argument("--out-inventory", required=True, help="Output path for the generated Ansible inventory file.")
args = parser.parse_args()

# load master config
try:
    with open(args.master_config, 'r') as f:
        master_config = json.load(f)
except (FileNotFoundError, json.JSONDecodeError) as e:
    print(f"Error: Could not load master config file '{args.master_config}'. {e}", file=sys.stderr)
    sys.exit(1)

target_regions = [r.strip() for r in args.regions.split(',')]


master_region_map = {node['region']: node for node in master_config['nodes']}
# # ensure all requested regions are in the master config and maintain order
# if not all(r in master_region_map for r in target_regions):
#     missing = [r for r in target_regions if r not in master_region_map]
#     print(f"Error: The following regions were not found in the master config: {missing}", file=sys.stderr)
#     sys.exit(1)

sub_nodes = [master_region_map[r] for r in target_regions]

# map from global_id to new sub_exp_id
global_id_to_sub_id = {node['global_id']: i for i, node in enumerate(sub_nodes)}

# if len(sub_nodes) != len(target_regions):
#     print("Error: Length mismatch, possible duplicate regions requested.", file=sys.stderr)
#     sys.exit(1)

# generate subconfig
sub_config = {}
sub_config['addresses'] = [[node['ip'], 8000] for node in sub_nodes]
sub_config['regions'] = [node['region'] for node in sub_nodes]

# extract submatrices
sub_latencies = []
for i in range(len(sub_nodes)):
    row = []
    global_row_idx = sub_nodes[i]['global_id']
    for j in range(len(sub_nodes)):
        global_col_idx = sub_nodes[j]['global_id']
        latency = master_config['raw_latencies'][global_row_idx][global_col_idx]
        row.append(latency)
    sub_latencies.append(row)
sub_config['raw_latencies'] = sub_latencies

# write toml file
try:
    with open(args.out_config, 'wb') as f:
        tomli_w.dump(sub_config, f)
except IOError as e:
    print(f"Error: Could not write to config file '{args.out_config}'. {e}", file=sys.stderr)
    sys.exit(1)

# write ansible inventory
try:
    with open(args.out_inventory, 'w') as f:
        f.write("[all:vars]\n")
        f.write("ansible_ssh_private_key_file=~/.ssh/kcensus_key\n")
        f.write("ansible_user=ec2-user\n\n")
        f.write("[kcensus_nodes]\n")
        for i, node in enumerate(sub_nodes):
            f.write(
                f"{node['ip']} "
                f"sub_exp_id={i} "
                f"cassandra_port={node['cassandra_port']}\n"
            )
except IOError as e:
    print(f"Error: Could not write to inventory file '{args.out_inventory}'. {e}", file=sys.stderr)
    sys.exit(1)