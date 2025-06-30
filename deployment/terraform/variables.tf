variable "target_regions" {
  description = "A list of AWS regions to deploy servers into."
  type        = list(string)
}

variable "ssh_public_key_path" {
  description = "Path to the SSH public key file."
  type        = string
}

variable "instance_type" {
  description = "The EC2 instance type to use for the servers."
  type        = string
  default     = "t3.micro"
}