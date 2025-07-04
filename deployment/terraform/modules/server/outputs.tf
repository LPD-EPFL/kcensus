# outputs of each server (public ip + instance id)

output "public_ip" {
  description = "The public IP address of the created server."
  value       = aws_instance.server.public_ip
}

output "instance_id" {
  description = "The ID of the created EC2 instance."
  value       = aws_instance.server.id
}