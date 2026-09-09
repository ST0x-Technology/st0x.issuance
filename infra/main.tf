module "prod" {
  source = "./modules/stack"

  environment            = "prod"
  do_token               = var.do_token
  droplet_size           = var.prod_droplet_size
  droplet_name           = "st0x-issuance-nixos"
  volume_name            = "st0x-issuance-data"
  volume_description     = "Persistent storage for SQLite database and logs (issuance)"
  api_source_addresses   = var.prod_api_source_addresses
  https_source_addresses = var.prod_https_source_addresses
}

module "staging" {
  source = "./modules/stack"

  environment            = "staging"
  do_token               = var.do_token
  droplet_size           = var.staging_droplet_size
  droplet_name           = "st0x-issuance-staging"
  volume_name            = "st0x-issuance-staging-data"
  api_source_addresses   = var.staging_api_source_addresses
  https_source_addresses = var.staging_https_source_addresses
}
