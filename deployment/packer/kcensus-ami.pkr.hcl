# This is how we build the AMI with the dependencies needed for the kcensus
# experiments and copy it to all regions

packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

source "amazon-ebs" "kcensus-node" {
  ami_name      = "kcensus-node-{{timestamp}}"
  instance_type = "t3.medium"
  region        = "eu-central-2"
  ssh_username  = "ec2-user"

  ami_regions = [
    "us-west-2",
    "ca-west-1",
    "ca-central-1",
    "us-west-1",
    "us-east-2",
    "us-east-1",
    "mx-central-1",
    "sa-east-1",
    "eu-west-3",
    "eu-west-2",
    "eu-west-1",
    "me-south-1",
    "eu-south-2",
    "eu-south-1",
    "eu-north-1",
    "eu-central-2",
    "eu-central-1",
    "il-central-1",
    "me-central-1",
    "af-south-1",
    "ap-south-2",
    "ap-south-1",
    "ap-southeast-1",
    "ap-northeast-2",
    "ap-northeast-1",
    "ap-east-1",
    "ap-east-2",
    "ap-southeast-3",
    "ap-southeast-7",
    "ap-southeast-5",
    "ap-northeast-3",
    "ap-southeast-2",
    "ap-southeast-4"
  ]

  source_ami_filter {
    filters = {
      name                = "al2023-ami-2023.*-kernel-*-x86_64"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["amazon"]
  }
}

build {
  name    = "kcensus-node-ami"
  sources = ["source.amazon-ebs.kcensus-node"]

  provisioner "shell" {
    inline = [
      "echo 'Waiting for dnf to be ready...'",
      "sleep 15",
      "sudo dnf update -y",
      "sudo dnf install -y docker time htop git cargo",
      "sudo systemctl enable docker",
      "sudo systemctl start docker",
      "sudo usermod -a -G docker ec2-user",
      "sudo docker pull shotover/cassandra-test:5.0-rc1-r3"
    ]
  }
}
