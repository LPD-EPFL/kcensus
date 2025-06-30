output "server_ips" {
  description = "A map of regions to the public IP addresses of the deployed servers."
  value = merge(
    # Americas
    contains(local.target_regions_set, "us-west-2") ? { "us-west-2" = module.server_stack_us_west_2[0].public_ip } : {},
    contains(local.target_regions_set, "ca-west-1") ? { "ca-west-1" = module.server_stack_ca_west_1[0].public_ip } : {},
    contains(local.target_regions_set, "ca-central-1") ? { "ca-central-1" = module.server_stack_ca_central_1[0].public_ip } : {},
    contains(local.target_regions_set, "us-west-1") ? { "us-west-1" = module.server_stack_us_west_1[0].public_ip } : {},
    contains(local.target_regions_set, "us-east-2") ? { "us-east-2" = module.server_stack_us_east_2[0].public_ip } : {},
    contains(local.target_regions_set, "us-east-1") ? { "us-east-1" = module.server_stack_us_east_1[0].public_ip } : {},
    contains(local.target_regions_set, "mx-central-1") ? { "mx-central-1" = module.server_stack_mx_central_1[0].public_ip } : {},

    # EMEA
    contains(local.target_regions_set, "eu-west-3") ? { "eu-west-3" = module.server_stack_eu_west_3[0].public_ip } : {},
    contains(local.target_regions_set, "eu-west-2") ? { "eu-west-2" = module.server_stack_eu_west_2[0].public_ip } : {},
    contains(local.target_regions_set, "eu-west-1") ? { "eu-west-1" = module.server_stack_eu_west_1[0].public_ip } : {},
    contains(local.target_regions_set, "me-south-1") ? { "me-south-1" = module.server_stack_me_south_1[0].public_ip } : {},
    contains(local.target_regions_set, "eu-south-2") ? { "eu-south-2" = module.server_stack_eu_south_2[0].public_ip } : {},
    contains(local.target_regions_set, "eu-south-1") ? { "eu-south-1" = module.server_stack_eu_south_1[0].public_ip } : {},
    contains(local.target_regions_set, "eu-north-1") ? { "eu-north-1" = module.server_stack_eu_north_1[0].public_ip } : {},
    contains(local.target_regions_set, "eu-central-2") ? { "eu-central-2" = module.server_stack_eu_central_2[0].public_ip } : {},
    contains(local.target_regions_set, "eu-central-1") ? { "eu-central-1" = module.server_stack_eu_central_1[0].public_ip } : {},
    contains(local.target_regions_set, "il-central-1") ? { "il-central-1" = module.server_stack_il_central_1[0].public_ip } : {},
    contains(local.target_regions_set, "me-central-1") ? { "me-central-1" = module.server_stack_me_central_1[0].public_ip } : {},
    contains(local.target_regions_set, "af-south-1") ? { "af-south-1" = module.server_stack_af_south_1[0].public_ip } : {},

    # APAC
    contains(local.target_regions_set, "ap-south-2") ? { "ap-south-2" = module.server_stack_ap_south_2[0].public_ip } : {},
    contains(local.target_regions_set, "ap-south-1") ? { "ap-south-1" = module.server_stack_ap_south_1[0].public_ip } : {},
    contains(local.target_regions_set, "ap-southeast-1") ? { "ap-southeast-1" = module.server_stack_ap_southeast_1[0].public_ip } : {},
    contains(local.target_regions_set, "ap-northeast-2") ? { "ap-northeast-2" = module.server_stack_ap_northeast_2[0].public_ip } : {},
    contains(local.target_regions_set, "ap-northeast-1") ? { "ap-northeast-1" = module.server_stack_ap_northeast_1[0].public_ip } : {},
    contains(local.target_regions_set, "ap-east-1") ? { "ap-east-1" = module.server_stack_ap_east_1[0].public_ip } : {},
    contains(local.target_regions_set, "ap-southeast-3") ? { "ap-southeast-3" = module.server_stack_ap_southeast_3[0].public_ip } : {},
    contains(local.target_regions_set, "ap-southeast-7") ? { "ap-southeast-7" = module.server_stack_ap_southeast_7[0].public_ip } : {},
    contains(local.target_regions_set, "ap-southeast-5") ? { "ap-southeast-5" = module.server_stack_ap_southeast_5[0].public_ip } : {},
    contains(local.target_regions_set, "ap-northeast-3") ? { "ap-northeast-3" = module.server_stack_ap_northeast_3[0].public_ip } : {},
    contains(local.target_regions_set, "ap-southeast-2") ? { "ap-southeast-2" = module.server_stack_ap_southeast_2[0].public_ip } : {}
  )
}