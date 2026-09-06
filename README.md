# KCensus

KCensus is a framework for strongly consistent geo-replication that synthesizes optimal consensus
fast-path schemes for a given network topology, workload, and latency objective. This repository includes
the framework, a geo-replicated key-value store built with it, and experiments across AWS regions
against competing protocols.

# Running Experiments & Reproducing Results

Reproduce the paper's plots from [archived logs (§4)](#4-reproducing-the-paper-plots-from-archived-logs),
or rerun experiments on AWS or locally.

Allow approximately 10 GB of disk space to store all experiment logs.

> **Without AWS:** follow §1, §2.2 (skip cloud tooling), §3.1, then
> [§5 Running Locally, Without AWS](#5-running-locally-without-aws).
> Local runs use simulated link delays and can closely reproduce the latency results, depending
> on your hardware. See §5 for results from our laptop runs.

## 1. Clone the Repository

First, clone the KCensus repository to your local machine:

```bash
git clone https://github.com/LPD-EPFL/kcensus/
cd kcensus
```

## 2. Environment Configuration

This guide assumes a Linux machine, used either to orchestrate AWS experiments or run them locally.

> §2.1 and §2.3 are AWS-only. Local runs need §2.2 without cloud tooling.

### 2.1 Cloud Prerequisites

- **AWS Credentials**: Artifact reviewers will receive credentials separately. To reproduce the
  experiments with your own account, create an [AWS account](https://aws.amazon.com/),
  [enable the regions used by the experiments](https://us-east-1.console.aws.amazon.com/billing/home?region=us-east-1#/account),
  and create a dedicated IAM user with `AmazonEC2FullAccess`.
- **AWS CLI**: Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) on
  your machine (`aws-cli-v2` on arch).

On Ubuntu 26.04:

```bash
sudo snap install aws-cli --classic
```

#### Configure AWS CLI

Enter the provided AWS access key ID and secret, or your own credentials:

```bash
aws configure
```

This saves credentials in `~/.aws/credentials`, which Terraform, Packer, and Ansible also use.

### 2.2 Local Machine Dependencies

Install the following tools:

* **Cloud tooling (AWS only)**:
    * [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) (`terraform` on arch)
    * [Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html) (`ansible` on arch)
    * [Packer](https://developer.hashicorp.com/packer/install) (`packer` on arch) (if you plan to
      rebuild the image)
* **Runtimes & Build Tools**:
    * [Python 3](https://www.python.org/downloads/), including `venv` and `pip` (`python3-venv` and
      `python3-pip` on Debian/Ubuntu; included with `python` on arch). The plotting scripts run in
      a virtual environment that `graphs/env.sh` creates on first use.
    * [Rust Toolchain](https://www.rust-lang.org/tools/install) (`rustup`, `cargo`)
* **Ansible Collection (AWS only)**:

    ```bash
    ansible-galaxy collection install community.general
    ```

On Ubuntu 26.04:

```bash
sudo apt update
sudo snap install terraform --classic
sudo snap install packer # On WSL, packer will not work if installed via snap
sudo apt install unzip ansible python3 python3-venv python3-pip rustup build-essential
ansible-galaxy collection install community.general
rustup default stable
```

**Optional — paper typography.** Without Linux Libertine, matplotlib warns and uses a fallback
font; the plotted values are unaffected. To install the bundled font:

```bash
mkdir -p ~/.local/share/fonts/otf && cp -r graphs/LinLibertine ~/.local/share/fonts/otf/
fc-cache && rm -rf ~/.cache/matplotlib
```

### 2.3 SSH Key Configuration

Generate a new SSH key pair **without a passphrase**:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/kcensus_key -N ""
```

Keep these paths: the evaluation scripts expect `~/.ssh/kcensus_key` and `~/.ssh/kcensus_key.pub`.

## 3. Building the Artifacts

### 3.1 Building the Binaries (Rust)

Compile the `kcensus` and `graph_bench` executables:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --target x86_64-unknown-linux-musl --release
```

> **CPU requirement.** `.cargo/config.toml` sets `-C target_cpu=x86-64-v3`, requiring an
> AVX2-era CPU (Haswell, 2013, or newer). The AWS instances used here support it. On older
> hardware, remove `.cargo/config.toml` and rebuild to avoid `SIGILL` ("Illegal instruction").
> The effect on performances should be minor.

### 3.2 (Optional) Building the Custom AMI (Packer)

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
`deployment/terraform/modules/server/main.tf` — it is keyed by region name, with one entry per
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

## 4. Reproducing the Paper Plots from Archived Logs

You do not need to rerun the experiments to reproduce the exact paper plots. The complete logs are
available in the [artifact-evaluation-v1 release](https://github.com/LPD-EPFL/kcensus/releases/tag/artifact-evaluation-v1).
From the root of a fresh clone, download them and generate the plots with:

```bash
curl -L -o logs.zip https://github.com/LPD-EPFL/kcensus/releases/download/artifact-evaluation-v1/logs.zip
unzip logs.zip
./plot.sh all
```

The figures and their underlying numbers are written to `graphs/plots/`. This only requires the
Python plotting dependencies from §2.2, not AWS credentials or a new experiment run. Install the
Linux Libertine font as described there if you also want the typography to match the paper.

## 5. Running Locally, Without AWS

Local runs use one process per replica and simulate link delays using the AWS latency snapshots
in `configs/`. They require Rust, Python with `venv` and `pip` (§2.2), and GNU `time` at
`/usr/bin/time` for resource measurements.

See §6 for experiment descriptions and figure mappings. Omit `--run-id` and add `--local` to
both commands:

```bash
./eval.sh --local exp-1      # instead of ./eval.sh --run-id reviewer-1 exp-1
./plot.sh --local exp-1      # instead of ./plot.sh exp-1
```

These commands build and run experiment 1 (~15 minutes), then plot Figures 1 and 7.
No cloud cleanup is needed.

To generate every figure:

```bash
./eval.sh --local all
./plot.sh --local all
```

Approximate local runtimes: exp-1 takes 15 minutes, exp-2 takes 1h45min, exp-3 takes 1h10min,
and exp-4 takes 1h50min (~5h total).

### What is different from the AWS runs

Local logs go to `./local-logs` instead of `./logs`, and figures have a `local-` prefix,
so they do not overwrite AWS results.

To fit on one machine, throughput is divided by `(f+1)/2`: half rate at 7 replicas, an eighth
at 31. Log paths record the reduced rate, so plotting requires `--local`.
Measurement windows are extended: experiments 1–3 collect at least a quarter of the AWS request
count, and experiment 4 matches it to keep compute measurements comparable.

### How to read the results

Local runs use saved AWS latency matrices and can match the original results more closely than
a fresh deployment, whose network delays may have changed. Differences in delays can still affect
which leaders and quorums are chosen, and which regions have the lowest latency.

- **Latency (Figures 1, 7, 8, 9):** on our laptop, local results closely matched experiments 1 and 2;
  agreement depends on your hardware. In Figure 7, 15 of 20 protocol/deployment averages were within 5% of the archived results,
  with a maximum difference of 8.1%. The fastest and slowest proposer latencies differed by up
  to 15.6%. Agreement can vary more at larger replica counts (experiment 3).
- **Traffic (Figure 10):** bytes per request are identical across local and AWS runs.
- **Memory (Figure 11):** usage is almost identical across local and AWS runs.
- **CPU time and optimization time (Figures 11 and 12):** absolute values depend on hardware.
  For reference, the paper used an `m5.16xlarge` for Figure 11 and a `t3.medium` for Figure 12.

## 6. Running Experiments on AWS

All commands below assume you are in the root `kcensus` directory.

Each experiment is run with `eval.sh`, which provisions the infrastructure, deploys the code,
runs the experiment and collects the logs. `plot.sh` then turns those logs into the figures.

> **Reproducibility note.** A fresh AWS run reproduces the experimental procedure, but is not
> guaranteed to produce the exact values reported in the paper. Inter-region latencies vary over
> time and between deployments, which can affect latency measurements, leader selection and quorum
> selection. Use the archived logs in §4 to reproduce the exact paper plots.

There are four experiments, numbered in the order their figures first appear in the paper. Each
figure is produced by exactly one experiment:

| Figure | Section                               | Experiment |
|:------:|---------------------------------------|:----------:|
|   1    | Introduction                          |  `exp-1`   |
|   7    | End-to-End Latency                    |  `exp-1`   |
|   8    | Impact of Failures on Latency         |  `exp-2`   |
|   9    | Scalability of Latency With Replicas  |  `exp-3`   |
|   12   | Time to Optimize Requirements         |  `exp-3`   |
|   10   | Resource Consumption (traffic)        |  `exp-4`   |
|   11   | Resource Consumption (CPU and memory) |  `exp-4`   |

To produce a figure, choose a run ID, run its experiment and then plot it. When several reviewers
share the AWS account, each must choose a different run ID:

```bash
./eval.sh --run-id reviewer-1 exp-N  # provision, run, collect the logs, tear down
./plot.sh exp-N                       # draw every figure that experiment produces
```

The run ID must contain 1–32 lowercase letters, digits or hyphens and start with a letter or digit.
It namespaces all AWS resources created by the command; it does not change the log paths or the subsequent `./plot.sh exp-N`
command. Running multiple AWS experiments in parallel requires separate clones and distinct run IDs
to keep Terraform state, logs, and cloud resources separate.

`plot.sh` produces all figures for an experiment from the same logs; run each experiment only once.

To run everything the paper depends on:

```bash
./eval.sh --run-id reviewer-1 all  # exp-1 .. exp-4
./plot.sh all                      # every figure
```

Running everything takes ~5h.

> **Note**: The experiments take several hours and incur AWS costs — `exp-2` (faults) dominates,
> and `exp-3` holds 31 instances across every region for its whole duration.
>
> Each experiment destroys its own resources when it finishes, and attempts to do so if it gives
> up on a run. **Do not rely on that.** If you interrupt a script, or anything else goes wrong,
> always check for surviving instances yourself — see
> [§7 Cleaning Up Cloud Resources](#7-crucial-cleaning-up-cloud-resources).

Each figure script writes a `.pdf` (the figure) and a `.txt` (the numbers behind it) into
`graphs/plots/`, named `exp-<experiment>-figure-<number>-<content>`:

---

### Experiment 1 — end-to-end latency (Figures 1 and 7)

Four 7-replica deployments: Northern Hemisphere, Europe, North America and East Asia.

```bash
./eval.sh --run-id reviewer-1 exp-1
./plot.sh exp-1
```

Approximate runtime: 30min.

*Outputs: `graphs/plots/exp-1-figure-1-intro.pdf` and `exp-1-figure-7-latency.pdf` (+ `.txt`)*

---

### Experiment 2 — impact of failures (Figure 8)

The Northern-Hemisphere deployment, with every combination of up to 3 crashed replicas.

```bash
./eval.sh --run-id reviewer-1 exp-2
./plot.sh exp-2
```

Approximate runtime: 2h30min.

*Output: `graphs/plots/exp-2-figure-8-faults.pdf` (+ `.txt`)*

> **Run order.** `exp-1` and `exp-2` share logs for six zero-fault `aws-ring-7` configurations;
> the last run overwrites them. Run `exp-1` before `exp-2` (as `all` does) to keep all Figure 8
> subfigures from the same deployment. If you run them in reverse order, rerun `exp-2`.

---

### Experiment 3 — scalability and optimization time (Figures 9 and 12)

Deployments from 3 to 31 replicas worldwide, using one 31-instance provisioning.

```bash
./eval.sh --run-id reviewer-1 exp-3
./plot.sh exp-3
```

Approximate runtime: 1h30min.

*Outputs: `graphs/plots/exp-3-figure-9-scalability.pdf` and `exp-3-figure-12-propagation.pdf`
(+ `.txt`)*

---

### Experiment 4 — resource consumption (Figures 10 and 11)

A single large machine, simulating link delays locally.

```bash
./eval.sh --run-id reviewer-1 exp-4
./plot.sh exp-4
```

Approximate runtime: 30min.

*Outputs: `graphs/plots/exp-4-figure-10-network.pdf` and `exp-4-figure-11-cpu-mem.pdf`
(+ `.txt`)*

## 7. Crucial: Cleaning Up Cloud Resources

**Always clean up resources to avoid unexpected AWS bills.**

`eval.sh` destroys resources after each experiment, but failures or interruptions can leave them running.

After a run or interruption, check for remaining instances:

```bash
./check-aws-cleanup.sh --run-id reviewer-1 # Check only your run
# OR
./check-aws-cleanup.sh                     # Check every KCensus run
```

The command lists remaining instances by AWS region, including their state and name. Names follow
`kcensus-<experiment-id>-<instance-aws-region>`. For `exp-1`, the experiment ID also includes
the deployment's region set (such as `aws-europe-7`); the final suffix identifies the individual
instance's AWS region (such as `eu-west-1`).
A `Cleanup verified` output means all matching instances are terminated.

### Manual Cleanup

Use `eval.sh destroy` with the deployment's Terraform variable file and `<experiment-id>` from
the instance name above (also printed during provisioning). The examples use the run ID `reviewer-1`;
if no run ID was used, remove the `-reviewer-1` suffix.

Example 1: if `exp-1` was interrupted on the Europe deployment:

For `kcensus-exp-1-aws-europe-7-reviewer-1-eu-west-1`, `<experiment-id>` is
`exp-1-aws-europe-7-reviewer-1` and `<instance-aws-region>` is `eu-west-1`.
For `exp-1`, choose the `.tfvars` file from the region set in `<experiment-id>`:

| Region set | Terraform variable file |
|---|---|
| `aws-ring-7` | `ring-7.tfvars` |
| `aws-europe-7` | `europe-7.tfvars` |
| `aws-north-america-7` | `north-america-7.tfvars` |
| `aws-east-asia-7` | `east-asia-7.tfvars` |

The cleanup command covers the whole deployment, across AWS regions:

```bash
./eval.sh destroy deployment/terraform/regions/europe-7.tfvars exp-1-aws-europe-7-reviewer-1
```

Example 2: if `exp-2` (faults) was interrupted:

```bash
./eval.sh destroy deployment/terraform/regions/ring-7.tfvars exp-2-reviewer-1
```

Example 3: if `exp-3` (scalability) or `exp-4` (resources) was interrupted:

```bash
./eval.sh destroy deployment/terraform/regions/aws-31.tfvars exp-3-reviewer-1
./eval.sh destroy deployment/terraform/regions/one.tfvars    exp-4-reviewer-1
```

Rerun `check-aws-cleanup.sh --run-id reviewer-1` after manual cleanup to verify that all instances are terminated.
