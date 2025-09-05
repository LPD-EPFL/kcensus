# This file is used only to display the server regions with their respective
# ip addresses in the terminal after they are deployed (only for inspection)

output "server_ips" {
  description = "A map of regions to the public IP addresses of the deployed servers."
  value       = local.deployed_ips
}