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
    "us-west-2"      = "ami-0b993f509b080cf93"
    "us-west-1"      = "ami-09b70372f1882a9fb"
    "us-east-2"      = "ami-06a58abfcff6fd4d0"
    "us-east-1"      = "ami-02e95d63b9bb39b68"
    "ca-west-1"      = "ami-0e1d188e327d74e11"
    "ca-central-1"   = "ami-00a65f1972e9b14e3"
    "mx-central-1"   = "ami-0dc5e125f2a873ac3"
    "eu-west-3"      = "ami-004d1134e985de383"
    "eu-west-2"      = "ami-02f30a47dcb5e2fd2"
    "eu-west-1"      = "ami-0b3eb50432f20e2d7"
    "eu-south-2"     = "ami-0d52376c44cf0f121"
    "eu-south-1"     = "ami-001c7e40dec4c9924"
    "eu-north-1"     = "ami-07ba730fdb0f94208"
    "eu-central-2"   = "ami-0982547d5aa30898d"
    "eu-central-1"   = "ami-0ea86dc732e5064f7"
    "il-central-1"   = "ami-04e98fe53ca45dd67"
    "me-south-1"     = "ami-099e83d628d75d32a"
    "me-central-1"   = "ami-0b1aca3af8dbdb643"
    "ap-south-2"     = "ami-08ef25a69598d6049"
    "ap-south-1"     = "ami-0e9b714c08e618165"
    "ap-southeast-1" = "ami-0d18eab391c9be44c"
    "ap-southeast-3" = "ami-0ab98d488f74f436d"
    "ap-southeast-7" = "ami-0cba04e9f8bea3bb1"
    "ap-southeast-5" = "ami-0d9c16e44e154469f"
    "ap-northeast-3" = "ami-0430f7826c8e63b01"
    "ap-northeast-2" = "ami-03ff9a562dbd0c166"
    "ap-southeast-2" = "ami-0eb812de9284243eb"
    "ap-southeast-4" = "ami-0291e7b54d0e5ff1d"
    "ap-northeast-1" = "ami-0bc8f90fb94b1fb41"
    "ap-east-1"      = "ami-0b796a6a431a9cd2d"
    "af-south-1"     = "ami-03053bce9a8063c0c"
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