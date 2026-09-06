# KCensus

KCensus is a faster alternative to Paxos-like consensus protocols.

# Running Experiments & Reproducing Results

This guide provides instructions to reproduce the plots from the KCensus paper. It covers:

* Configuring your local environment and AWS account
* Building required artifacts (binaries and custom machine image)
* Launching experiments and generating plots

The workflow is automated using Packer, Terraform, Ansible, and shell scripts.

> **No AWS account? Every experiment can also run on a single machine**, with the wide-area link
> delays simulated. It needs only the Rust toolchain and Python — no cloud credentials, no cost —
> and produces all the same figures. It is a check that the pipeline and the protocols behave,
> not a reproduction of the reported numbers: read "How to read the results" in §4 before drawing
> conclusions from a local figure.
>
> If that is what you are after, the route is: §1 (clone), §2.2 (dependencies — you can skip the
> Terraform/Ansible/Packer entries), §3.1 (build), then
> [§4 Running Locally, Without AWS](#4-running-locally-without-aws), which tells you how the
> commands in §5 map onto the local runner. §2.1, §2.3, §2.4, §3.2 and §6 are AWS-only.

## 1. Clone the Repository

First, clone the KCensus repository to your local machine:

```bash
git clone https://github.com/LPD-EPFL/kcensus/
cd kcensus
```

## 2. Environment Configuration

The experiments run on AWS but are orchestrated from your local machine. This README assumes that
your machine is running Linux.

> Running locally instead (§4)? You still need **§2.2**, minus its Terraform, Ansible and Packer
> entries. §2.1, §2.3 and §2.4 are AWS-only.

### 2.1 Cloud Prerequisites

- **AWS Account**: Create an AWS account at [aws.amazon.com](https://aws.amazon.com/)
  and [enable all AWS regions](https://us-east-1.console.aws.amazon.com/billing/home?region=us-east-1#/account).
- **IAM User**: In the AWS Console (on your browser), create a dedicated IAM user with permissions for EC2 (policy
  `AmazonEC2FullAccess`) (and AMI if you plan to rebuild the image (policy `AWSImageBuilderFullAccess`)).
- **AWS CLI**: Install the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) on
  your machine (`aws-cli-v2` on arch).

#### Configure AWS CLI

In the AWS management console, go to IAM service, navigate to the IAM user you created in the previous step, and create
an access key for it and note the ID and secret. Then run this command in your terminal:

```bash
aws configure
```

This will set up your credentials in `~/.aws/credentials`.

### 2.2 Local Machine Dependencies

Install the following tools:

* **Infrastructure & Automation**:
    * [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli) (`terraform` on arch)
    * [Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html) (`ansible` on arch)
    * [Packer](https://learn.hashicorp.com/tutorials/packer/get-started-install-cli) (`packer` on arch) (if you plan to
      rebuild the image)
* **Runtimes & Build Tools**:
    * [Python 3](https://www.python.org/downloads/), including `venv` and `pip` (`python3-venv` and
      `python3-pip` on Debian/Ubuntu; included with `python` on arch). The plotting scripts run in
      a virtual environment that `graphs/env.sh` creates on first use.
    * [Rust Toolchain](https://www.rust-lang.org/tools/install) (`rustup`, `cargo`)
* **Ansible Collection**:
  After successfully installing Ansible, run this command:
    ```bash
    ansible-galaxy collection install community.general
    ```

**Optional — matching the paper's typography.** The figures are drawn in Linux Libertine. If the
font is not installed, matplotlib prints `findfont: Font family 'Linux Libertine O' not found` and
falls back to a default face: the figures are still numerically correct, only the lettering
differs. To silence the warnings and match the paper exactly:

```bash
mkdir -p ~/.local/share/fonts/otf && cp -r graphs/LinLibertine ~/.local/share/fonts/otf/
fc-cache && rm -rf ~/.cache/matplotlib
```

### 2.3 SSH Key Configuration

Generate a new SSH key pair **without a passphrase**:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/kcensus_key -N ""
```

- Private key: `~/.ssh/kcensus_key`
- Public key: `~/.ssh/kcensus_key.pub`

Keep these exact file paths: the evaluation scripts expect `~/.ssh/kcensus_key` and `~/.ssh/kcensus_key.pub`.

### 2.4 Linking Terraform, Packer, and Ansible to AWS

If you managed to do all the previous steps successfully, then Terraform, Ansible, and Packer will all work seamlessly
with your AWS account without further configuration.

## 3. Building the Artifacts

### 3.1 Building the Binaries (Rust)

Compile the `kcensus` and `graph_bench` executables:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --target x86_64-unknown-linux-musl --release
```

> **CPU requirement.** `.cargo/config.toml` builds with `-C target_cpu=x86-64-v3`, so the binaries
> require an AVX2-era CPU (Haswell, 2013, or newer). This never affects the AWS runs — the
> `t3.medium` and `m5.16xlarge` instances used by the experiments both support it — but a binary
> *run* on older hardware dies with `SIGILL` ("Illegal instruction").
>
> If that applies to you, delete `.cargo/config.toml` and rebuild. Nothing else reads that file;
> the build simply falls back to the portable baseline. It affects speed, not results.

### 3.2 (Optional) Building the Custom AMI (Packer)

You only need this section if you want to build replacement AMIs; otherwise, you can skip this step as we provide pre-built AMIs in all AWS regions with every dependency required by the remote machines. The experiment binaries are uploaded separately, so changing them does not require rebuilding the AMIs.

```bash
cd deployment/packer
packer init .
packer build kcensus-ami.pkr.hcl
cd ../..
```

This step builds the AMI in one AWS region and copies it to others. It may take ~30 minutes.

Packer prints one AMI ID per region at the end. Copy them into the `ami_ids` map in
`deployment/terraform/modules/server/main.tf` — it is keyed by region name, with one entry per
region the experiments can deploy to (31 today):

```hcl
locals {
  ami_ids = {
    "af-south-1" = "ami-0d3fdcf99c39e3437"
    "ap-east-1"  = "ami-0d1ed5125e83f96a0"
    ...
  }
}
```

## 4. Running Locally, Without AWS

Every experiment can also run on a single machine, with all replicas as local processes and the
wide-area link delays simulated from the latency matrices in `configs/`, measured earlier from the
same AWS regions. This needs no AWS account and no credentials. It does not reproduce the paper's
numbers — see "How to read the results" below for what does and does not carry over.

**§5 is still the reference for what each experiment does**, which figures it produces and where
they are written. Everything there applies unchanged — just add `--local` to both commands:

```bash
./eval.sh --local exp-3      # instead of ./eval.sh exp-3
./plot.sh --local exp-3     # instead of ./plot.sh exp-3
```

The experiment and plot names are identical either way. There is no provisioning step and nothing
to clean up afterwards, so §6 does not apply.

To check the whole pipeline works before committing to a long run:

```bash
./eval.sh --local exp-1              # ~10 minutes: build, run, and produce Figures 1 and 7
./plot.sh --local exp-1
```

and the full set, if you want every figure:

```bash
./eval.sh --local all                # ~5 hours
./plot.sh --local all
```

When running locally, you can expect ~10min to run exp-1, ~1h40min to run exp-2, ~1h05min to run exp-3 and ~1h45min to run exp-4, for a total of ~4h40min.

Requirements are the same as §2.2 minus the cloud tooling: the Rust toolchain, Python with `venv`,
and GNU `time` at `/usr/bin/time` (the resource figures parse its output).

### What is different from the AWS runs

**Results never mix with the AWS ones.** Local runs write to `./local-logs`, never `./logs`, and
`plot.sh --local` writes figures with a `local-` prefix. Running locally cannot overwrite results
collected on AWS.

**Throughput is reduced.** One machine cannot sustain the aggregate rate of a wide-area
deployment, so local runs divide it by `(f+1)/2` — half rate at 7 replicas, an eighth at 31. The
log path records the reduced value it actually used (`t=500`, `t=125`), which is why `plot.sh`
needs the `--local` flag to find it.

**Measurement windows are stretched to compensate.** A lower rate over a fixed 10 s window would
collect ever fewer requests as the deployment grows, so the window grows instead: experiments 1–3
collect at least a quarter of the requests an AWS run does at every size, and experiment 4
reproduces the request count *exactly*, because it measures compute and compute tracks requests
processed.

### How to read the results

**The simulated link delays are not the ones the paper measured.** An AWS run pings the
instances it just provisioned and builds its latency matrix from those measurements, so every
deployment gets a slightly different one. A local run cannot do that; it uses the matrices checked
into `configs/`, which are a snapshot taken from the same regions at some earlier point. Close,
but not identical.

That matters more than the size of the difference suggests, because **the protocols pick their
leaders and quorums from that matrix by taking a minimum**, and when two candidates are within a
few percent a small change in the pings flips a discrete choice. Concretely, in our local run of
experiment 1, multi-paxos on `aws-east-asia-7` elected Osaka, where the AWS run elected Taipei —
the two are within 7% of each other on the metric that decides it. Every subsequent latency in
that subfigure is then measured against a different leader. The chosen leader is printed as
`Leader: <pid>` at the top of each server's log, so you can check this yourself.

So for the **latency figures (1, 7, 8, 9)**, expect the deployment-wide averages to land close and
the detail to move around. In our local run of experiment 1 the per-protocol averages were within
8% of the AWS ones, and usually within 5% — but the fastest and slowest proposer, which is what
the whiskers show, differed by as much as 15%, and individual regions can reorder.

**Figure 10** (traffic and communication) should carry over cleanly: bytes and messages per application request are
properties of the protocol, not of the machine or of the
exact delays.

**Figures 11 and 12 depend on your hardware**, and are the two that will not match. Figure 11
reports CPU time and memory; Figure 12 reports the time to compute optimal requirements on a
single core. The paper measured them on different instances — an `m5.16xlarge` for Figure 11, and
one of experiment 3's `t3.medium` machines for Figure 12 — so expect the absolute values to differ
by roughly the performance ratio between your machine and those. A modern laptop is typically
faster than either, so the numbers will usually come out lower.

What carries over is the shape of the curves: how cost grows with the replica count, and how the
protocols compare with each other.

Treat a local run as a check that the pipeline and the protocols behave, not as a reproduction of
the reported numbers.

## 5. Running Experiments on AWS

All commands below assume you are in the root `kcensus` directory.

Each experiment is run with `eval.sh`, which provisions the infrastructure, deploys the code,
runs the experiment and collects the logs. `plot.sh` then turns those logs into the figures.

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

To produce a figure, run its experiment and then plot it:

```bash
./eval.sh exp-N     # provision, run, collect the logs, tear down
./plot.sh exp-N     # draw every figure that experiment produces
```

When several reviewers share the AWS account, each must choose a different run ID:

```bash
./eval.sh --run-id reviewer-1 exp-N
```

The run ID may contain 1–32 lowercase letters, digits or hyphens. It namespaces all AWS resources
created by the command; it does not change the log paths or the subsequent `./plot.sh exp-N`
command. Each reviewer should run from a separate clone, because Terraform state and logs are
local to the clone.

Note where an experiment appears twice above: `exp-1` and `exp-3` each produce **two figures, in
two different sections of the paper**, from a single set of runs. `plot.sh` draws both at once —
there is no need to run the experiment again for the second figure, and for `exp-3` that would
mean provisioning 31 instances across every region a second time.

To run everything the paper depends on:

```bash
./eval.sh all       # exp-1 .. exp-4
./plot.sh all       # every figure
```

Running everything takes approximately ~4h30.

> **Note**: The experiments take several hours and incur AWS costs — `exp-2` (faults) dominates,
> and `exp-3` holds 31 instances across every region for its whole duration.
>
> Each experiment destroys its own resources when it finishes, and attempts to do so if it gives
> up on a run. **Do not rely on that.** If you interrupt a script, or anything else goes wrong,
> always check for surviving instances yourself — see
> [§6 Cleaning Up Cloud Resources](#6-crucial-cleaning-up-cloud-resources).

Each figure script writes a `.pdf` (the figure) and a `.txt` (the numbers behind it) into
`graphs/plots/`, named `exp-<experiment>-figure-<number>-<content>`:

---

### Experiment 1 — end-to-end latency (Figures 1 and 7)

Four 7-replica deployments: Northern Hemisphere, Europe, North America and East Asia.

```bash
./eval.sh exp-1
./plot.sh exp-1
```

The eval of exp-1 takes approximately 20min to run.

*Outputs: `graphs/plots/exp-1-figure-1-intro.pdf` and `exp-1-figure-7-latency.pdf` (+ `.txt`)*

---

### Experiment 2 — impact of failures (Figure 8)

The Northern-Hemisphere deployment, with every combination of up to 3 crashed replicas.

```bash
./eval.sh exp-2
./plot.sh exp-2
```

The eval of exp-2 takes approximately 2h20min to run.

*Output: `graphs/plots/exp-2-figure-8-faults.pdf` (+ `.txt`)*

> **Run order.** `exp-2` needs a zero-fault baseline for its "0 Faults" subfigure, and re-runs the
> same six `aws-ring-7` configurations `exp-1` already ran. Both write to the same log directory,
> so the last one to run wins — which is why `all` runs `exp-1` first.
>
> Running `exp-1` *after* `exp-2` is therefore not wrong, but it leaves Figure 8's 0-fault
> subfigure measured on a different deployment from the 1-, 2- and 3-fault ones. The effect is
> small and does not bias the comparison: every algorithm within a subfigure is still measured on
> the same deployment, so only the overall level of the 0-fault bars can shift, never the ordering
> between algorithms. Re-run `exp-2` afterwards if you want all four subfigures from one
> deployment, as in the paper.

---

### Experiment 3 — scalability and optimization time (Figures 9 and 12)

Deployments from 3 to 31 replicas worldwide. Both figures come from this one 31-instance
provisioning, which is why they are bundled.

```bash
./eval.sh exp-3
./plot.sh exp-3
```

The eval of exp-3 takes approximately 1h30min to run.

*Outputs: `graphs/plots/exp-3-figure-9-scalability.pdf` and `exp-3-figure-12-propagation.pdf`
(+ `.txt`)*

---

### Experiment 4 — resource consumption (Figures 10 and 11)

A single large machine, simulating link delays locally.

```bash
./eval.sh exp-4
./plot.sh exp-4
```

The eval of exp-4 takes approximately 30min to run.

*Outputs: `graphs/plots/exp-4-figure-10-network.pdf` and `exp-4-figure-11-cpu-mem.pdf`
(+ `.txt`)*

## 6. Crucial: Cleaning Up Cloud Resources

**Always clean up resources to avoid unexpected AWS bills.**

The `eval.sh` script automatically destroys resources after each experiment. But if not terminated correctly or
interrupted, resources may remain running.

### Manual Cleanup

To manually clean up resources, use the `destroy` command of `eval.sh`. Provide the Terraform variable file and the
full experiment ID printed during provisioning. When `--run-id reviewer-1` was used, append
`-reviewer-1` to the IDs below.

`exp-1` provisions one deployment per region set, so its experiment ID carries the deployment
name; the others use a single ID.

Example 1: if `exp-1` was interrupted on the Europe deployment:

```bash
./eval.sh destroy deployment/terraform/regions/europe-7.tfvars exp-1-aws-europe-7
# With --run-id reviewer-1: exp-1-aws-europe-7-reviewer-1
```

Example 2: if `exp-2` (faults) was interrupted:

```bash
./eval.sh destroy deployment/terraform/regions/ring-7.tfvars exp-2
# With --run-id reviewer-1: exp-2-reviewer-1
```

Example 3: if `exp-3` (scalability) or `exp-4` (resources) was interrupted:

```bash
./eval.sh destroy deployment/terraform/regions/aws-31.tfvars exp-3
./eval.sh destroy deployment/terraform/regions/one.tfvars    exp-4
```

**Always verify in the AWS EC2 Console** that all instances with "kcensus" in their name have been terminated after you
are finished.

> **Note**: If the account is only used for this project, you can use the following
> link [VPC Console](https://console.aws.amazon.com/vpcconsole/home) and expand the **See all regions** list under *
*Running instances** to find all running instances across all regions.
