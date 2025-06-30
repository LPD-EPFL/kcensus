locals {
  target_regions_set     = toset(var.target_regions)
  ssh_public_key_content = file(var.ssh_public_key_path)
}

# helper to find own public IP address. Fetched only once.
data "http" "my_ip" {
  url = "http://ipv4.icanhazip.com"
}

# Americas
module "server_stack_us_west_2" { # Oregon
  count          = contains(local.target_regions_set, "us-west-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.us-west-2 }
  region         = "us-west-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ca_west_1" { # CA Calgary
  count          = contains(local.target_regions_set, "ca-west-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ca-west-1 }
  region         = "ca-west-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ca_central_1" { # CA Central
  count          = contains(local.target_regions_set, "ca-central-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ca-central-1 }
  region         = "ca-central-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_us_west_1" { # N. California
  count          = contains(local.target_regions_set, "us-west-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.us-west-1 }
  region         = "us-west-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_us_east_2" { # Ohio
  count          = contains(local.target_regions_set, "us-east-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.us-east-2 }
  region         = "us-east-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_us_east_1" { # N. Virginia
  count          = contains(local.target_regions_set, "us-east-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.us-east-1 }
  region         = "us-east-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_mx_central_1" { # Mexico
  count          = contains(local.target_regions_set, "mx-central-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.mx-central-1 }
  region         = "mx-central-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

# Europe, Middle East & Africa (EMEA)
module "server_stack_eu_west_3" { # Paris
  count          = contains(local.target_regions_set, "eu-west-3") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-west-3 }
  region         = "eu-west-3"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_west_2" { # London
  count          = contains(local.target_regions_set, "eu-west-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-west-2 }
  region         = "eu-west-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_west_1" { # Ireland
  count          = contains(local.target_regions_set, "eu-west-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-west-1 }
  region         = "eu-west-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_me_south_1" { # Bahrain
  count          = contains(local.target_regions_set, "me-south-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.me-south-1 }
  region         = "me-south-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_south_2" { # Spain
  count          = contains(local.target_regions_set, "eu-south-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-south-2 }
  region         = "eu-south-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_south_1" { # Milan
  count          = contains(local.target_regions_set, "eu-south-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-south-1 }
  region         = "eu-south-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_north_1" { # Stockholm
  count          = contains(local.target_regions_set, "eu-north-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-north-1 }
  region         = "eu-north-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_central_2" { # Zurich
  count          = contains(local.target_regions_set, "eu-central-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-central-2 }
  region         = "eu-central-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_eu_central_1" { # Frankfurt
  count          = contains(local.target_regions_set, "eu-central-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.eu-central-1 }
  region         = "eu-central-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_il_central_1" { # Tel Aviv
  count          = contains(local.target_regions_set, "il-central-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.il-central-1 }
  region         = "il-central-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_me_central_1" { # UAE
  count          = contains(local.target_regions_set, "me-central-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.me-central-1 }
  region         = "me-central-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_af_south_1" { # Cape Town
  count          = contains(local.target_regions_set, "af-south-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.af-south-1 }
  region         = "af-south-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

# Asia-Pacific (APAC)
module "server_stack_ap_south_2" { # Hyderabad
  count          = contains(local.target_regions_set, "ap-south-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-south-2 }
  region         = "ap-south-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_south_1" { # Mumbai
  count          = contains(local.target_regions_set, "ap-south-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-south-1 }
  region         = "ap-south-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_southeast_1" { # Singapore
  count          = contains(local.target_regions_set, "ap-southeast-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-southeast-1 }
  region         = "ap-southeast-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_northeast_2" { # Seoul
  count          = contains(local.target_regions_set, "ap-northeast-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-northeast-2 }
  region         = "ap-northeast-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_northeast_1" { # Tokyo
  count          = contains(local.target_regions_set, "ap-northeast-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-northeast-1 }
  region         = "ap-northeast-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_east_1" { # Hong Kong
  count          = contains(local.target_regions_set, "ap-east-1") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-east-1 }
  region         = "ap-east-1"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_southeast_3" { # Jakarta
  count          = contains(local.target_regions_set, "ap-southeast-3") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-southeast-3 }
  region         = "ap-southeast-3"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_southeast_7" { # Thailand
  count          = contains(local.target_regions_set, "ap-southeast-7") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-southeast-7 }
  region         = "ap-southeast-7"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_southeast_5" { # Malaysia
  count          = contains(local.target_regions_set, "ap-southeast-5") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-southeast-5 }
  region         = "ap-southeast-5"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_northeast_3" { # Osaka
  count          = contains(local.target_regions_set, "ap-northeast-3") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-northeast-3 }
  region         = "ap-northeast-3"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

module "server_stack_ap_southeast_2" { # Sydney
  count          = contains(local.target_regions_set, "ap-southeast-2") ? 1 : 0
  source         = "./modules/server"
  providers      = { aws = aws.ap-southeast-2 }
  region         = "ap-southeast-2"
  instance_type  = var.instance_type
  ssh_public_key = local.ssh_public_key_content
  my_ip_for_ssh  = chomp(data.http.my_ip.response_body)
}

# Local provisioner to manage inventory.ini
resource "null_resource" "manage_inventory" {
  depends_on = [
    module.server_stack_us_west_2,
    module.server_stack_ca_west_1,
    module.server_stack_ca_central_1,
    module.server_stack_us_west_1,
    module.server_stack_us_east_2,
    module.server_stack_us_east_1,
    module.server_stack_mx_central_1,
    module.server_stack_eu_west_3,
    module.server_stack_eu_west_2,
    module.server_stack_eu_west_1,
    module.server_stack_me_south_1,
    module.server_stack_eu_south_2,
    module.server_stack_eu_south_1,
    module.server_stack_eu_north_1,
    module.server_stack_eu_central_2,
    module.server_stack_eu_central_1,
    module.server_stack_il_central_1,
    module.server_stack_me_central_1,
    module.server_stack_af_south_1,
    module.server_stack_ap_south_2,
    module.server_stack_ap_south_1,
    module.server_stack_ap_southeast_1,
    module.server_stack_ap_northeast_2,
    module.server_stack_ap_northeast_1,
    module.server_stack_ap_east_1,
    module.server_stack_ap_southeast_3,
    module.server_stack_ap_southeast_7,
    module.server_stack_ap_southeast_5,
    module.server_stack_ap_northeast_3,
    module.server_stack_ap_southeast_2
  ]

  triggers = {
    server_ips = jsonencode(local.deployed_ips)
  }

  provisioner "local-exec" {
    command = <<-EOF
      cat > ../ansible/inventory.ini << EOL
[kcensus_nodes]
%{for ip in values(local.deployed_ips)~}
${ip}
%{endfor~}
EOL
    EOF
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<-EOF
      cat > ../ansible/inventory.ini << EOL
[kcensus_nodes]
EOL
    EOF
  }
}

locals {
  deployed_ips = merge(
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