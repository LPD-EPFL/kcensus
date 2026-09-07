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
    "af-south-1"     = "ami-0d3fdcf99c39e3437"
    "ap-east-1"      = "ami-0d1ed5125e83f96a0"
    "ap-east-2"      = "ami-049812da0b0e9270f"
    "ap-northeast-1" = "ami-01d9994cadb39b732"
    "ap-northeast-2" = "ami-01b072277bc2a7315"
    "ap-northeast-3" = "ami-0d200f7a8471e7e00"
    "ap-south-1"     = "ami-0509b291bd019ad2f"
    "ap-south-2"     = "ami-0a09b02e61c34bd0f"
    "ap-southeast-1" = "ami-062c085aede251ac4"
    "ap-southeast-2" = "ami-0e7c5833f80fb8e01"
    "ap-southeast-3" = "ami-0cd8b69aba572e8fe"
    "ap-southeast-4" = "ami-00742397d3576af25"
    "ap-southeast-5" = "ami-07da4f42e8d567e52"
    "ap-southeast-7" = "ami-092281d98f8280190"
    "ca-central-1"   = "ami-074e2f833c9e92ac8"
    "ca-west-1"      = "ami-0ed9d1d55ee3205c1"
    "eu-central-1"   = "ami-046aeca1ecb3efed2"
    "eu-central-2"   = "ami-0ea69311cd8064e71"
    "eu-north-1"     = "ami-0e1cb523a9f65e81e"
    "eu-south-1"     = "ami-03a570b8349bee2e0"
    "eu-south-2"     = "ami-0fd731477d0694268"
    "eu-west-1"      = "ami-02f339de0a0dd5925"
    "eu-west-2"      = "ami-0002f1b9c636315ea"
    "eu-west-3"      = "ami-09525cb348d9e6729"
    "il-central-1"   = "ami-08d3afcfcf5d465b9"
    "mx-central-1"   = "ami-073bd087acddd468a"
    "sa-east-1"      = "ami-0f100e2affb68059c"
    "us-east-1"      = "ami-071795a46516e46f5"
    "us-east-2"      = "ami-0893958614d309d2f"
    "us-west-1"      = "ami-04e411bfe09c18779"
    "us-west-2"      = "ami-03e343ad959ac80cd"
  }
}

resource "aws_key_pair" "kcensus_key" {
  key_name   = "kcensus-key-${var.experiment_id}-${var.region}"
  public_key = var.ssh_public_key
}

resource "aws_security_group" "kcensus_sg" {
  name        = "kcensus-sg-${var.experiment_id}-${var.region}"
  description = "Allow SSH and internal traffic for nodes in ${var.region} for experiment ${var.experiment_id}"

  # rule for Ansible: allow SSH from the control machine. Open to the internet rather than pinned
  # to that machine's IP: a deployment outlives a VPN reconnect or a DHCP lease, and a pinned rule
  # would lock Ansible out of live instances mid-experiment. Authentication is key-only.
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow SSH from everywhere (key-only auth)"
  }

  # rule for inter-node ping (ICMP) for latency checks
  ingress {
    from_port   = -1
    to_port     = -1
    protocol    = "icmp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow ICMP (ping) from everywhere"
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

# Which datacentre a region's instance lands in. Left to AWS it can differ between runs, and
# inter-zone latency differs by a millisecond or two -- enough to flip a leader or a quorum,
# both of which are minima over the measured matrix.
#
# Pinned by zone *id* rather than name: `us-west-2a` is a different building in every account,
# `usw2-az1` is the same one everywhere. Only zones offering the instance type are considered,
# since not all of them do.
data "aws_ec2_instance_type_offerings" "zones" {
  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
  location_type = "availability-zone-id"
}

data "aws_subnet" "pinned" {
  availability_zone_id = sort(data.aws_ec2_instance_type_offerings.zones.locations)[0]
  default_for_az       = true
}

resource "aws_instance" "server" {
  ami = local.ami_ids[var.region]

  instance_type          = var.instance_type
  key_name               = aws_key_pair.kcensus_key.key_name
  subnet_id              = data.aws_subnet.pinned.id
  vpc_security_group_ids = [aws_security_group.kcensus_sg.id]

  tags = {
    Name         = "kcensus-${var.experiment_id}-${var.region}"
    ExperimentID = var.experiment_id
  }
}
