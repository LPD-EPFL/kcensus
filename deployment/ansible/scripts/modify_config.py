#!/usr/bin/env python3

# parse input toml, replace current host ip with 0.0.0.0:8000

import sys
import re

def usage():
    print("Usage: modify_config.py <input.toml> <sub_exp_id> <current_host_ip>", file=sys.stderr)
    sys.exit(1)

if len(sys.argv) != 4:
    usage()

input_path = sys.argv[1]
try:
    sub_id = int(sys.argv[2])
except ValueError:
    usage()

# TODO: remove this since we don't use it anymore
current_host_ip = sys.argv[3]

with open(input_path, 'r') as f:
    content = f.read()

lines = content.split('\n')
output_lines = []
in_addresses = False
address_count = 0
skip_lines = 0

for line in lines:
    if skip_lines > 0:
        skip_lines -= 1
        continue
    if line.strip().startswith('addresses = ['):
        in_addresses = True
        output_lines.append(line)
        continue
    elif in_addresses and line.strip() == ']':
        in_addresses = False
        output_lines.append(line)
        continue
    elif in_addresses:
        if line.strip().startswith('['):
            if address_count == sub_id:
                output_lines.append('    [')
                output_lines.append('        "0.0.0.0",')
                output_lines.append('        8000,')
                output_lines.append('    ],')
                skip_lines = 3
                address_count += 1
            else:
                output_lines.append(line)
                address_count += 1
        else:
            output_lines.append(line)
    else:
        output_lines.append(line)

print('\n'.join(output_lines), end='')