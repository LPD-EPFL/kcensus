# K-Census

Fast Consensus Through Knowledge Gossiping

# Running Experiments & Reproducing Results

This guide provides instructions to reproduce the plots from the K-Census paper. It covers:

*   Configuring your local environment and AWS account
*   Building required artifacts (binaries and custom machine image)
*   Launching experiments and generating plots

The workflow is automated using Packer, Terraform, Ansible, and shell scripts.


## 1. Clone the Repository

First, clone the K-Census repository to your local machine:

```bash
git clone https://github.com/lPD-EPFL/k-census/
cd k-census
```


## 2. Environment Configuration

All experiments are managed from a single "gateway" machine (your computer), which orchestrates cloud resources on AWS. This guide assumes you are using a linux machine.

### 2.1 Cloud Prerequisites

- **AWS Account**: Create an AWS account at [aws.amazon.com](https://aws.amazon.com/).
- **IAM User**: In the AWS Console (on your browser), create a dedicated IAM user with permissions for EC2, AMI, VPC, and Security Groups.
- **AWS CLI**: Install the [AWS CLI](https://aws.amazon.com/cli/) on your machine.

#### Configure AWS CLI

Run this command and enter your IAM user's credentials:

```bash
aws configure
```

This will set up your credentials in `~/.aws/credentials`.

### 2.2 Local Machine Dependencies

Install the following tools:

* **Infrastructure & Automation**:
    * [Terraform](https://learn.hashicorp.com/tutorials/terraform/install-cli)
    * [Packer](https://learn.hashicorp.com/tutorials/packer/get-started-install-cli)
    * [Ansible](https://docs.ansible.com/ansible/latest/installation_guide/intro_installation.html)
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
ssh-keygen -t rsa -b 4096 -f ~/.ssh/kcensus_key -N ""
```

- Private key: `~/.ssh/kcensus_key`
- Public key: `~/.ssh/kcensus_key.pub`

#### Link it to AWS

1. In the AWS Console (on your browser), go to EC2 > Key Pairs.
2. Click "Import Key Pair".
3. Name it `kcensus_key` and paste the contents of `~/.ssh/kcensus_key.pub`. you can get the contents of this file by running the following command on your terminal:

```bash
cat ~/.ssh/kcensus_key.pub
```

You should not call your key something other than `kcensus_key` because it is hardcoded in the Terraform configuration.


### 2.4 Linking Terraform, Packer, and Ansible to AWS

If you managed to do all the previous steps successfully, then Terraform, Ansible, and Packer will all work seamlessly with your AWS account without further configuration.


## 3. Building the Artifacts

### 3.1 Build the Custom AMI (Packer)

The custom AMI contains all dependencies for remote machines. You need to run the following commands:

```bash
cd deployment/packer
packer build kcensus-ami.pkr.hcl
cd ../..
```

This step builds the AMI in one AWS region and copies it to others. It may take ~30 minutes.

### 3.2 Build the Binaries (Rust)

Compile the `kcensus` and `graph_bench` executables:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --target x86_64-unknown-linux-musl --release
```


## 4. Running Experiments and Generating Plots

All commands below assume you are in the root `k-census` directory.

Each experiment is run using `geo_eval.sh`, which provisions infrastructure, deploys code, runs the experiment, and collects logs. The `plot.sh` script processes logs to generate plot data.

> **Note**: Running these experiments will take several hours and incur AWS costs (especially exp-4 and 3-5). Each experiment automatically cleans up its resources upon completion. See last section for what to do in case a script is interrupted for whatever reason.

---

### Figure 4:

```bash
./geo_eval.sh exp-1
./plot.sh plot-1
```
*Output: `graphs/plots/1-pure-latency.txt`*

---

### Figure 6:

Note that in order to run this experiment, you must run exp-1 first.

```bash
./geo_eval.sh exp-2
./plot.sh plot-2
```
*Output: `graphs/plots/2-load-latency.txt`*

---

### Figure 5:

```bash
./geo_eval.sh exp-4
./plot.sh plot-4
```
*Output: `graphs/plots/4-faults.txt`*

---

### Figures 7 & 10

```bash
./geo_eval.sh exp-3-5
./plot.sh plot-3   # fig 7
./plot.sh plot-5   # fig 10
```
*Outputs: `graphs/plots/3-scalability.txt`, `graphs/plots/5-propagation.txt`*

---

### Figures 8 & 9

```bash
./geo_eval.sh exp-6
./plot.sh plot-6
```
*Outputs: `graphs/plots/6-network.txt`, `graphs/plots/7-cpu-mem.txt`*



## 5. Crucial: Cleaning Up Cloud Resources

**Always clean up resources to avoid unexpected AWS bills.**

The `geo_eval.sh` script automatically destroys resources after each experiment. But if not terminated correctly or interrupted, resources may remain running.

### Manual Cleanup

To manually clean up resources, use the `destroy` function in `geo_eval.sh`. Provide the Terraform variable file and the experiment ID used during provisioning.

Example 1: If `exp-1` was interrupted on `aws-europe-7`:

```bash
./geo_eval.sh destroy deployment/terraform/regions/europe-7.tfvars exp-1-aws-europe-7
```

Example 2: If `exp-2` was interrupted:

```bash
./geo_eval.sh destroy deployment/terraform/regions/world-ring-13.tfvars exp-2
```

**Always verify in the AWS EC2 Console** that all instances with "kcensus" in their name have been terminated after you are finished.

>**Note**: If the account is only used for this project, you can use the following link [VPC Console](https://console.aws.amazon.com/vpcconsole/home) and expand the **See all regions** list under **Running instances** to find all running instances across all regions.