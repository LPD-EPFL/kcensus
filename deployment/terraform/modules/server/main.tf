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

data "aws_ami" "kcensus_node" {
  most_recent = true
  owners      = ["self"]

  filter {
    name   = "name"
    values = ["kcensus-node-*"]
  }
}

resource "aws_instance" "server" {
  ami                    = data.aws_ami.kcensus_node.id

  instance_type          = var.instance_type
  key_name               = aws_key_pair.kcensus_key.key_name
  vpc_security_group_ids = [aws_security_group.kcensus_sg.id]

  tags = {
    Name         = "kcensus-${var.experiment_id}-${var.region}"
    ExperimentID = var.experiment_id
  }
}