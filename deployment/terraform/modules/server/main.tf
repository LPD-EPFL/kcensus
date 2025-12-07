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
    "af-south-1" = "ami-0cbd0c10bf973c2d6"
    "ap-east-1" = "ami-053afd7aeceae7e09"
    "ap-northeast-1" = "ami-0c0f148c557e4ba96"
    "ap-northeast-2" = "ami-05ce39fa97d123558"
    "ap-northeast-3" = "ami-02fc1b7d0e9138107"
    "ap-south-1" = "ami-0e7c948b6a2fa6ea1"
    "ap-south-2" = "ami-01f9d28720910fe66"
    "ap-southeast-1" = "ami-08c37edb3cdb5e046"
    "ap-southeast-2" = "ami-0a5e6d10fd7aa6992"
    "ap-southeast-3" = "ami-0e98b7689f00daab1"
    "ap-southeast-4" = "ami-0450647c36d19a8af"
    "ap-southeast-5" = "ami-0400c7aae1bf09e67"
    "ap-southeast-7" = "ami-00f70869550993c77"
    "ca-central-1" = "ami-00fdfcf68f97f03ce"
    "ca-west-1" = "ami-0fadec739994aa396"
    "eu-central-1" = "ami-0bdc5a755ed3aa8a4"
    "eu-central-2" = "ami-0d6c603124d232640"
    "eu-north-1" = "ami-0c8c5b3bd55239001"
    "eu-south-1" = "ami-08e844361394a5e9f"
    "eu-south-2" = "ami-0ba881d3414db7481"
    "eu-west-1" = "ami-0f443ff48e90cb6f2"
    "eu-west-2" = "ami-04c14856da0496029"
    "eu-west-3" = "ami-099673693c8d1e8d2"
    "il-central-1" = "ami-095eb9b5aa1a184ed"
    "me-central-1" = "ami-00cf55b809498e3ec"
    "me-south-1" = "ami-02dfeec08a5db2c6d"
    "mx-central-1" = "ami-096633c5bfe46aec7"
    "us-east-1" = "ami-05c49b75824858f14"
    "us-east-2" = "ami-0194140fd5922c8b1"
    "us-west-1" = "ami-045a822af43ef6e65"
    "us-west-2" = "ami-01222b0cc0ba99281"
    "ap-east-2": "ami-025232a54426a0641"
    "sa-east-1": "ami-00ace8e2c51a2ff53"
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
