# input variables required by the server module

variable "region" {
  description = "The AWS region where resources will be created."
  type        = string
}

variable "ssh_public_key" {
  description = "Contents of the SSH public key."
  type        = string
  sensitive   = true
}

variable "instance_type" {
  description = "The EC2 instance type."
  type        = string
}

variable "my_ip_for_ssh" {
  description = "The public IP of the user for the SSH ingress rule."
  type        = string
}