# KCensus

KCensus is a faster alternative to Paxos-like consensus protocols.

# Running Experiments & Reproducing Results

This guide provides instructions to reproduce the plots from the KCensus paper. It covers:

* Configuring your local environment and AWS account
* Building required artifacts (binaries and custom machine image)
* Launching experiments and generating plots

The workflow is automated using Packer, Terraform, Ansible, and shell scripts.

## 1. Clone the Repository

First, clone the KCensus repository to your local machine:

```bash
git clone https://github.com/LPD-EPFL/kcensus/
cd kcensus
```

## 2. Environment Configuration

All experiments run on AWS, but they are orchestrated from your local machine. This README assumes that your machine is
running Linux.

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
    * [Python 3](https://www.python.org/downloads/)
    * [Rust Toolchain](https://www.rust-lang.org/tools/install) (`rustup`, `cargo`)
* **Ansible Docker Collection**:
  After successfully installing Ansible, run this command:
    ```bash
    ansible-galaxy collection install community.docker
    ```

### 2.3 SSH Key Configuration

Generate a new SSH key pair **without a passphrase**:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/kcensus_key -N ""
```

- Private key: `~/.ssh/kcensus_key`
- Public key: `~/.ssh/kcensus_key.pub`

#### Link it to AWS

1. In the AWS Console (on your browser), go to EC2 > Key Pairs.
2. Click "Import Key Pair".
3. Name it `kcensus_key` and paste the contents of `~/.ssh/kcensus_key.pub`. you can get the contents of this file by
   running the following command on your terminal:

```bash
cat ~/.ssh/kcensus_key.pub
```

You should not call your key something other than `kcensus_key` because it is hardcoded in the Terraform configuration.

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

### 3.2 Building the Custom AMI (Packer)

We use a custom image that contains all dependencies required by remote machines.
This image does **not** include the binaries, which are uploaded during the evaluation; as a result, there is no need to
rebuild the image when binaries change.
We provide pre-built AMIs, available in all AWS regions.
In case you need to build a new image, run the following commands:

```bash
cd deployment/packer
packer init .
packer build kcensus-ami.pkr.hcl
cd ../..
```

This step builds the AMI in one AWS region and copies it to others. It may take ~30 minutes.
Then, update AMIs in `deployment/terraform/modules/server/main.tf`.

## 4. Running Experiments and Generating Plots

All commands below assume you are in the root `kcensus` directory.

Each experiment is run with `geo_eval.sh`, which provisions the infrastructure, deploys the code,
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
./geo_eval.sh exp-N     # provision, run, collect the logs, tear down
./plot.sh     plot-N    # draw every figure that experiment produces
```

Note where an experiment appears twice above: `exp-1` and `exp-3` each produce **two figures, in
two different sections of the paper**, from a single set of runs. `plot.sh` draws both at once —
there is no need to run the experiment again for the second figure, and for `exp-3` that would
mean provisioning 31 instances across every region a second time.

To run everything the paper depends on:

```bash
./geo_eval.sh all     # exp-1 .. exp-4
./plot.sh all         # every figure
```

> **Note**: The experiments take several hours and incur AWS costs — `exp-2` (faults) dominates,
> and `exp-3` holds 31 instances across every region for its whole duration.
>
> Each experiment destroys its own resources when it finishes, and attempts to do so if it gives
> up on a run. **Do not rely on that.** If you interrupt a script, or anything else goes wrong,
> always check for surviving instances yourself — see
> [§5 Cleaning Up Cloud Resources](#5-crucial-cleaning-up-cloud-resources).

Each figure script writes a `.pdf` (the figure) and a `.txt` (the numbers behind it) into
`graphs/plots/`, named `exp-<experiment>-figure-<number>-<content>`:

---

### Experiment 1 — end-to-end latency (Figures 1 and 7)

Four 7-replica deployments: Northern Hemisphere, Europe, North America and East Asia.

```bash
./geo_eval.sh exp-1
./plot.sh plot-1
```

*Outputs: `graphs/plots/exp-1-figure-1-intro.pdf` and `exp-1-figure-7-latency.pdf` (+ `.txt`)*

---

### Experiment 2 — impact of failures (Figure 8)

The Northern-Hemisphere deployment, with every combination of up to 3 crashed replicas.

```bash
./geo_eval.sh exp-2
./plot.sh plot-2
```

*Output: `graphs/plots/exp-2-figure-8-faults.pdf` (+ `.txt`)*

---

### Experiment 3 — scalability and optimization time (Figures 9 and 12)

Deployments from 3 to 31 replicas worldwide. Both figures come from this one 31-instance
provisioning, which is why they are bundled.

```bash
./geo_eval.sh exp-3
./plot.sh plot-3
```

*Outputs: `graphs/plots/exp-3-figure-9-scalability.pdf` and `exp-3-figure-12-propagation.pdf`
(+ `.txt`)*

---

### Experiment 4 — resource consumption (Figures 10 and 11)

A single large machine, simulating link delays locally.

```bash
./geo_eval.sh exp-4
./plot.sh plot-4
```

*Outputs: `graphs/plots/exp-4-figure-10-network.pdf` and `exp-4-figure-11-cpu-mem.pdf`
(+ `.txt`)*

## 5. Crucial: Cleaning Up Cloud Resources

**Always clean up resources to avoid unexpected AWS bills.**

The `geo_eval.sh` script automatically destroys resources after each experiment. But if not terminated correctly or
interrupted, resources may remain running.

### Manual Cleanup

To manually clean up resources, use the `destroy` function in `geo_eval.sh`. Provide the Terraform variable file and the
experiment ID used during provisioning.

`exp-1` provisions one deployment per region set, so its experiment ID carries the deployment
name; the others use a single ID.

Example 1: if `exp-1` was interrupted on the Europe deployment:

```bash
./geo_eval.sh destroy deployment/terraform/regions/europe-7.tfvars exp-1-aws-europe-7
```

Example 2: if `exp-2` (faults) was interrupted:

```bash
./geo_eval.sh destroy deployment/terraform/regions/ring-7.tfvars exp-2
```

Example 3: if `exp-3` (scalability) or `exp-4` (resources) was interrupted:

```bash
./geo_eval.sh destroy deployment/terraform/regions/aws-31.tfvars exp-3
./geo_eval.sh destroy deployment/terraform/regions/one.tfvars    exp-4
```

**Always verify in the AWS EC2 Console** that all instances with "kcensus" in their name have been terminated after you
are finished.

> **Note**: If the account is only used for this project, you can use the following
> link [VPC Console](https://console.aws.amazon.com/vpcconsole/home) and expand the **See all regions** list under *
*Running instances** to find all running instances across all regions.
