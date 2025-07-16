#!/usr/bin/env python3
"""
modify_config.py <input.toml> <sub_exp_id>

Parses the TOML via tomllib, replaces addresses[sub_exp_id] → ["0.0.0.0", 8000],
then writes back the full TOML document, preserving everything else.
"""

import sys
import tomllib

def usage():
    print("Usage: modify_config.py <input.toml> <sub_exp_id>", file=sys.stderr)
    sys.exit(1)

if len(sys.argv) != 3:
    usage()

input_path = sys.argv[1]
try:
    sub_id = int(sys.argv[2])
except ValueError:
    usage()

text = open(input_path, 'rb').read()
conf = tomllib.loads(text.decode('utf-8'))
addrs = conf.get('addresses')
if not isinstance(addrs, list):
    print("ERROR: no top level 'addresses' list found", file=sys.stderr)
    sys.exit(1)
if sub_id < 0 or sub_id >= len(addrs):
    print(f"ERROR: sub_exp_id {sub_id} is out of range (0..{len(addrs)-1})",
          file=sys.stderr)
    sys.exit(1)


conf['addresses'][sub_id] = ["0.0.0.0", 8000]

def dump_toml(d):
    lines = []
    for key in d:
        val = d[key]
        if isinstance(val, list) and key == 'addresses':
            lines.append(f"{key} = [")
            for entry in val:
                ip, port = entry
                lines.append(f"  [ \"{ip}\", {port}, ],")
            lines.append("]")
        else:
            import json
            js = json.dumps(val, indent=2)
            js = js.replace('true', 'true').replace('false', 'false')
            lines.append(f"{key} = {js}")
        lines.append("")
    return "\n".join(lines)

print(dump_toml(conf), end="")