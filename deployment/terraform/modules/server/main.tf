# Reusable code that is used to create a single server node (the EC2 instace +
# security group + ssh key)

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
  }
}

locals {
  ami_ids = {
    "af-south-1": "ami-0d20eeed0736689e4"
    "ap-east-1": "ami-0286db28104e4a808"
    "ap-east-2": "ami-011de6bdbb1475467"
    "ap-northeast-1": "ami-0e7c3031c7345c9ed"
    "ap-northeast-2": "ami-02e4d1f60ba94abda"
    "ap-northeast-3": "ami-060bc5e6223b76680"
    "ap-south-1": "ami-016d1f71b2137bd5a"
    "ap-south-2": "ami-03227be5604a93cf0"
    "ap-southeast-1": "ami-039fdf5e840455cd2"
    "ap-southeast-2": "ami-0f4ff031cfeec82af"
    "ap-southeast-3": "ami-0c814b23366f89a82"
    "ap-southeast-4": "ami-0cb394f18de996410"
    "ap-southeast-5": "ami-0f96d9a1bb2874a9e"
    "ap-southeast-7": "ami-06a5eafb54dd6b87a"
    "ca-central-1": "ami-081ab12bd74f7b498"
    "ca-west-1": "ami-017e1ea587a46b323"
    "eu-central-1": "ami-04332e9a30bb8fdd3"
    "eu-central-2": "ami-00805f7b477b2f61a"
    "eu-north-1": "ami-0d97bc44c9dfe71e0"
    "eu-south-1": "ami-01527f70d95784b72"
    "eu-south-2": "ami-0089ab3af7e6651ae"
    "eu-west-1": "ami-0256b21bd535a4014"
    "eu-west-2": "ami-0d921c1c8196055ff"
    "eu-west-3": "ami-0cc44d68d8f700d23"
    "il-central-1": "ami-08a766bd41e6d99bb"
    "me-central-1": "ami-01aff7e7bdd985aab"
    "me-south-1": "ami-0e5b15c0b23ca4144"
    "mx-central-1": "ami-0aa3b2de9963c9553"
    "sa-east-1": "ami-03756dead42c567c0"
    "us-east-1": "ami-02ecbbde5b906ac65"
    "us-east-2": "ami-0c1337b79f326720f"
    "us-west-1": "ami-00661f96d4bf8f578"
    "us-west-2": "ami-04dab87e39cae2486"
  }
}

resource "aws_key_pair" "kcensus_key" {
  key_name   = "kcensus-key-${var.experiment_id}-${var.region}"
  public_key = var.ssh_public_key
}

resource "aws_security_group" "kcensus_sg" {
  name        = "kcensus-sg-${var.experiment_id}-${var.region}"
  description = "Allow SSH and internal traffic for nodes in ${var.region} for experiment ${var.experiment_id}"

  # rule for Ansible: allow SSH from control machine
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.my_ip_for_ssh}/32"]
    description = "Allow SSH from my IP"
  }

  # rule for inter-node ping (ICMP) for latency checks
  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
    description     = "Allow ICMP (ping) from everywhere"
  }

  # rule for application traffic on port 8000 and 9000
  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow kcensus node-to-node traffic"
  }

  ingress {
    from_port   = 9000
    to_port     = 9000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow kcensus node-to-node traffic"
  }

  # allow all outbound traffic from the instances
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound traffic"
  }

  tags = {
    Name         = "kcensus-sg-${var.experiment_id}-${var.region}"
    ExperimentID = var.experiment_id
  }
}

resource "aws_instance" "server" {
  ami                    = local.ami_ids[var.region]

  instance_type          = var.instance_type
  key_name               = aws_key_pair.kcensus_key.key_name
  vpc_security_group_ids = [aws_security_group.kcensus_sg.id]

  tags = {
    Name         = "kcensus-${var.experiment_id}-${var.region}"
    ExperimentID = var.experiment_id
  }
}
