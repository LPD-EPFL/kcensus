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

variable "experiment_id" {
  description = "A unique identifier for the experiment run."
  type        = string
}