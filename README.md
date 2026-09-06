# KCensus

KCensus is a framework for strongly consistent geo-replication that synthesizes optimal consensus
fast-path schemes for a given network topology, workload, and latency objective. This repository includes
the framework, a geo-replicated key-value store built with it, and experiments across AWS regions
against competing protocols.

See the [source map](src/README.md) for the implementation components and their relation to the paper.

# Running Experiments & Reproducing Results

Ways to reproduce the results:

- [Plot archived logs (§2)](#2-reproducing-the-paper-plots-from-archived-logs) to reproduce the paper's figures exactly.
- [Run locally (§5)](#5-running-locally-without-aws) with simulated link delays.
- [Run on AWS (§6)](#6-running-experiments-on-aws) to repeat the deployed experiments.

Start with §1 for all three workflows. Running new experiments also requires the build setup in §4.
Allow approximately 10 GB of disk space to store all experiment logs.

## 1. Getting Started

This guide assumes Linux. All commands run from the repository root unless stated otherwise.

```bash
git clone https://github.com/LPD-EPFL/kcensus/
cd kcensus
```

### Plotting dependencies

Install [Python 3](https://www.python.org/downloads/) with `venv` and `pip`, plus `curl` and
`unzip` to download the archived logs. On Ubuntu 26.04:

```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip curl unzip
```

The plotting scripts install their
Python packages in a virtual environment managed by `graphs/env.sh` on first use.

**Figure typography.** Install the bundled Linux Libertine font to match the paper's layout.
Fallback fonts can make text too large and cause labels to overlap:

```bash
mkdir -p ~/.local/share/fonts/otf && cp -r graphs/LinLibertine ~/.local/share/fonts/otf/
fc-cache && rm -rf ~/.cache/matplotlib
```

## 2. Reproducing the Paper Plots from Archived Logs

Complete §1 first to clone the repository and install the plotting dependencies.

You do not need to rerun the experiments to reproduce the exact paper plots. The complete logs are
available in the [artifact-evaluation-v1 release](https://github.com/LPD-EPFL/kcensus/releases/tag/artifact-evaluation-v1).
From the root of a fresh clone, download them and generate the plots with:

```bash
curl -L -o logs.zip https://github.com/LPD-EPFL/kcensus/releases/download/artifact-evaluation-v1/logs.zip
unzip logs.zip
./plot.sh all
```

The figures (`.pdf`) and their underlying numbers (`.txt`) are written to `graphs/plots/`.
No Rust build or AWS setup is needed.

The commands above use the same `logs/` directory and generate the same plot filenames as the AWS runs in §6.
Running those experiments and plotting their results overwrites the corresponding archived logs
and figures. Make a copy if you want to keep both.

## 3. Experiments and Outputs

The same four experiments, described in the table below, are available locally and on AWS. To run
them, complete the [build setup (§4)](#4-build-setup-for-new-experiments), then follow
[local runs (§5)](#5-running-locally-without-aws) or [AWS runs (§6)](#6-running-experiments-on-aws).

| Experiment | Description | Figures | Local runtime | AWS runtime |
|---|---|---|---|---|
| `exp-1` | End-to-end latency in four 7-replica deployments: Northern Hemisphere, Europe, North America, and East Asia | 1, 7 | 15min | 30min |
| `exp-2` | Northern Hemisphere, with every combination of up to 3 crashed replicas | 8 | 1h45min | 2h30min |
| `exp-3` | Latency scaling from 3 to 31 replicas worldwide, and requirements optimization time | 9, 12 | 1h10min | 1h30min |
| `exp-4` | Traffic, CPU, and memory consumption on a single machine with simulated link delays | 10, 11 | 1h50min | 30min |

Runtimes are approximate, totaling about 5 hours for either workflow.

Each figure has a `.pdf` and a `.txt` containing its underlying numbers in `graphs/plots/`:

| Experiment | Output filenames (before `.pdf` or `.txt`) |
|---|---|
| `exp-1` | `exp-1-figure-1-intro`, `exp-1-figure-7-latency` |
| `exp-2` | `exp-2-figure-8-faults` |
| `exp-3` | `exp-3-figure-9-scalability`, `exp-3-figure-12-propagation` |
| `exp-4` | `exp-4-figure-10-network`, `exp-4-figure-11-cpu-mem` |

Local figure filenames have an additional `local-` prefix.

The TOML files in `configs/` define each topology's regions and latency matrix (`raw_latencies`,
in milliseconds). The `EXP1_CONFIGS`, `EXP2_CONFIG`, `EXP3_TYPES`/`EXP3_SIZES`, and
`EXP4_TYPE`/`EXP4_SIZES` variables near the top of `lib.sh` select the topologies used by each
experiment.

## 4. Build Setup for New Experiments

For both local and AWS runs, install the [Rust toolchain](https://www.rust-lang.org/tools/install)
and build tools on your machine. Local resource measurements also require GNU `time` at
`/usr/bin/time`. On Ubuntu 26.04:

```bash
sudo apt install rustup build-essential time
rustup default stable
```

For local runs, compile the `kcensus` and `graph_bench` executables for your machine:

```bash
cargo build --release
```

For AWS runs, build static Linux binaries for the remote instances:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --target x86_64-unknown-linux-musl --release
```

> **CPU requirement.** `.cargo/config.toml` sets `-C target_cpu=x86-64-v3`, requiring an
> AVX2-era CPU (Haswell, 2013, or newer). The AWS instances used here support it. On older
> hardware, remove `.cargo/config.toml` and rebuild to avoid `SIGILL` ("Illegal instruction").
> The effect on performance should be minor.

## 5. Running Locally, Without AWS

Local runs use one process per replica and simulate link delays using the AWS latency snapshots
in `configs/`. Complete §1 and §4 first, then run an experiment and plot its results:

```bash
./eval.sh --local exp-1
./plot.sh --local exp-1
```

These commands build and run experiment 1 (~15 minutes), then plot Figures 1 and 7.
No cloud cleanup is needed.

All experiments, their figures, and estimated runtimes are listed in the [table in §3](#3-experiments-and-outputs).

To generate every figure:

```bash
./eval.sh --local all
./plot.sh --local all
```

The full suite takes approximately 5 hours.

### What is different from the AWS runs

Local logs go to `./local-logs` instead of `./logs`, and figures have a `local-` prefix,
so they do not overwrite AWS results.

To fit on one machine, throughput is divided by `(f+1)/2`: half rate at 7 replicas, an eighth
at 31. Log paths record the reduced rate, so plotting requires `--local`.
Measurement windows are extended: experiments 1-3 collect at least a quarter of the AWS request
count, and experiment 4 matches it to keep compute measurements comparable.

### How to read the results

Local runs use latency snapshots from AWS regions and can match the original results more closely than
a fresh deployment, whose network delays may have changed. Differences in delays can still affect
which leaders and quorums are chosen, and which regions have the lowest latency.

- **Latency (Figures 1, 7, 8, 9):** on our laptop, local results closely matched experiments 1 and 2. In Figure 7, protocol/deployment averages differed from the archived results by 3% on average. Differences can be larger at larger replica counts (experiment 3), or depending on topology and hardware.
- **Traffic (Figure 10):** bytes per request are identical across local and AWS runs.
- **Memory (Figure 11):** usage is almost identical across local and AWS runs.
- **CPU time and optimization time (Figures 11 and 12):** absolute values depend on hardware.
  For reference, the paper used an `m5.16xlarge` for Figure 11 and a `t3.medium` for Figure 12.

## 6. Running Experiments on AWS

Complete §1 and §4 first, then configure the cloud tools below. Local runs do not need this section.
On AWS, `eval.sh` provisions infrastructure, deploys the binaries, runs the experiment, collects
logs, and tears down the deployment. Experiment 3 provisions 31 instances; experiment 4 uses
one `m5.16xlarge` with simulated link delays.

> **Reproducibility note.** A fresh AWS run reproduces the experimental procedure, but is not
> guaranteed to produce the exact values reported in the paper. Inter-region latencies vary over
> time and between deployments, which can affect latency measurements, leader selection and quorum
> selection. Use the archived logs in §2 to reproduce the exact paper plots.

### 6.1 Deployment Tools

Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html),
[Terraform](https://developer.hashicorp.com/terraform/install), and
[Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html).
On Ubuntu 26.04:

```bash
sudo snap install aws-cli --classic
sudo snap install terraform --classic
sudo apt install ansible
ansible-galaxy collection install community.general
```

Install [Packer](https://developer.hashicorp.com/packer/install) only if you need to rebuild the
images. On Ubuntu 26.04:

```bash
sudo snap install packer # On WSL, use a non-snap installation
```

### 6.2 AWS Credentials

Artifact reviewers will receive credentials separately. To reproduce the experiments with your
own account, create an [AWS account](https://aws.amazon.com/),
[enable the regions used by the experiments](https://us-east-1.console.aws.amazon.com/billing/home?region=us-east-1#/account),
and create a dedicated IAM user with `AmazonEC2FullAccess`.

Enter the AWS access key ID and secret using:

```bash
aws configure
```

This saves credentials in `~/.aws/credentials`, which Terraform, Packer, and Ansible also use.

### 6.3 SSH Key

Generate a new SSH key pair **without a passphrase**:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/kcensus_key -N ""
```

Keep these paths: the evaluation scripts expect `~/.ssh/kcensus_key` and `~/.ssh/kcensus_key.pub`.

### 6.4 Run and Plot

Use the same experiment names as in §5, replacing `--local` with `--run-id reviewer-1` for
evaluation and omitting `--local` for plotting. Replace `exp-N` below with an experiment from
the [table in §3](#3-experiments-and-outputs), which lists all experiments, their figures, and estimated runtimes.
When several reviewers share the AWS account, each must choose a different run ID:

```bash
./eval.sh --run-id reviewer-1 exp-N  # provision, run, collect the logs, tear down
./plot.sh exp-N                       # draw every figure that experiment produces
```

The run ID should follow the shape of `reviewer-1` (e.g. `reviewer-2`, `reviewer-3`, ...).
It namespaces all AWS resources created by the command; it does not change the log paths or the subsequent `./plot.sh exp-N`
command. Running multiple AWS experiments in parallel requires separate clones and distinct run IDs
to keep Terraform state, logs, and cloud resources separate.

`plot.sh` produces all figures for an experiment from the same logs; run each experiment only once.

> **Run order.** `exp-1` and `exp-2` share logs for all zero-fault `aws-ring-7` configurations; the
> last run overwrites them. Prefer running `exp-1` before `exp-2` (as `all` does) to keep all
> Figure 8 subfigures from the same deployment (same instances, same latencies); rerun `exp-2` if
> you ran them in reverse order.

To run everything the paper depends on:

```bash
./eval.sh --run-id reviewer-1 all  # exp-1 .. exp-4
./plot.sh all                      # every figure
```

Running everything takes ~5h.

> **Note**: The experiments take several hours and incur AWS costs - `exp-2` (faults) dominates,
> and `exp-3` holds 31 instances across every region for its whole duration.
>
> Each experiment destroys its own resources when it finishes, and attempts to do so if it gives
> up on a run. **Do not rely on that.** If you interrupt a script, or anything else goes wrong,
> always check for surviving instances yourself - see
> [§6.5 Cleaning Up Cloud Resources](#65-crucial-cleaning-up-cloud-resources).

### 6.5 Crucial: Cleaning Up Cloud Resources

**Always clean up resources to avoid unexpected AWS bills.**

`eval.sh` destroys resources after each experiment, but failures or interruptions can leave them running.

After a run or interruption, check for remaining instances:

```bash
./check-aws-cleanup.sh --run-id reviewer-1 # Check only your run
# OR
./check-aws-cleanup.sh                     # Check every KCensus run
```

The command lists remaining instances by AWS region, including their state and name. Names follow
`kcensus-<experiment-id>-<instance-aws-region>` (also printed during provisioning). For `exp-1`,
the experiment ID also includes the deployment's region set (such as `aws-europe-7`); the final
suffix identifies the individual instance's AWS region (such as `eu-west-1`).
A `Cleanup verified` output means all matching instances are terminated.

#### Manual Cleanup

Use `eval.sh destroy <tfvars-path> <experiment-id>`, where `<experiment-id>` comes from the instance
names listed above, and `<tfvars-path>` depends on the experiment, as shown below. The examples use
the run ID `reviewer-1`; replace it with your own, or, if no run ID was used, remove the
`-reviewer-1` suffix.

`exp-1` is the only experiment with multiple deployments, so it's also the only one where
`<tfvars-path>` varies. For example, if it was interrupted on the Europe deployment:

For the instance name `kcensus-exp-1-aws-europe-7-reviewer-1-eu-west-1`, `<experiment-id>` is
`exp-1-aws-europe-7-reviewer-1` and `<instance-aws-region>` is `eu-west-1`.
Choose `<tfvars-path>` from the region set in `<experiment-id>`:

| Region set | Terraform variable file |
|---|---|
| `aws-ring-7` | `ring-7.tfvars` |
| `aws-europe-7` | `europe-7.tfvars` |
| `aws-north-america-7` | `north-america-7.tfvars` |
| `aws-east-asia-7` | `east-asia-7.tfvars` |

This single command tears down every instance in one region set:

```bash
./eval.sh destroy deployment/terraform/regions/europe-7.tfvars exp-1-aws-europe-7-reviewer-1
```

The other experiments each use a single, fixed `.tfvars` file, so you only need to replace the `<experiment-id>` with your own:

```bash
./eval.sh destroy deployment/terraform/regions/ring-7.tfvars exp-2-reviewer-1  # exp-2 (faults)
./eval.sh destroy deployment/terraform/regions/aws-31.tfvars exp-3-reviewer-1  # exp-3 (scalability)
./eval.sh destroy deployment/terraform/regions/one.tfvars    exp-4-reviewer-1  # exp-4 (resources)
```

Rerun `check-aws-cleanup.sh --run-id reviewer-1` after manual cleanup to verify that all instances are terminated.

### 6.6 Optional: Rebuild the AMIs

Pre-built AMIs include the remote dependencies in every region used by the experiments.
Rebuild them only if you need replacement images. Binaries are uploaded separately, so code
changes do not require rebuilding AMIs.

```bash
cd deployment/packer
packer init .
packer build kcensus-ami.pkr.hcl
cd ../..
```

This step builds the AMI in one AWS region and copies it to others. It may take ~30 minutes.

Packer prints one AMI ID per region at the end. Copy them into the `ami_ids` map in
`deployment/terraform/modules/server/main.tf` - it is keyed by region name, with one entry per
region the experiments can deploy to:

```hcl
locals {
  ami_ids = {
    "af-south-1" = "ami-0d3fdcf99c39e3437"
    "ap-east-1"  = "ami-0d1ed5125e83f96a0"
    ...
  }
}
```
