#!/usr/bin/env python3

import pathlib
import sys

# This file is needed to convert logs to correct format accepted by graphing scripts

ROOT_DIRECTORY = '../logs/'

def process_log_file(log_file_path: pathlib.Path):

    try:
        content = log_file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"[ERROR] Could not read file {log_file_path}: {e}", file=sys.stderr)
        return

    stdout_marker = "STDOUT:"
    stderr_marker = "STDERR:"

    stdout_content = ""
    stderr_content = ""

    if stderr_marker in content:
        parts = content.split(stderr_marker, 1)
        before_stderr = parts[0]
        stderr_content = parts[1].strip()
    else:
        before_stderr = content

    if stdout_marker in before_stderr:
        parts = before_stderr.split(stdout_marker, 1)
        stdout_content = parts[1].strip()

    if log_file_path.name == "graph_bench.log":
        stdout_file_name = "graph_bench.stdout"
        stderr_file_name = "graph_bench.stderr"
    else:
        digits_only_name = "".join(filter(str.isdigit, log_file_path.stem))
        stdout_file_name = f"{digits_only_name}.stdout"
        stderr_file_name = f"{digits_only_name}.stderr"

    stdout_file_path = log_file_path.parent / stdout_file_name
    stderr_file_path = log_file_path.parent / stderr_file_name

    if stdout_content:
        try:
            stdout_file_path.write_text(stdout_content, encoding='utf-8')
        except Exception as e:
            print(f"[ERROR] Could not write file {stdout_file_path}: {e}", file=sys.stderr)

    if stderr_content:
        try:
            stderr_file_path.write_text(stderr_content, encoding='utf-8')
        except Exception as e:
            print(f"  [ERROR] Could not write file {stderr_file_path}: {e}", file=sys.stderr)


search_path = pathlib.Path(ROOT_DIRECTORY)
log_files = list(search_path.rglob('*.log'))
for log_file in log_files:
    process_log_file(log_file)