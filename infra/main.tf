module "prod" {
  source = "./modules/stack"

  environment        = "prod"
  do_token           = var.do_token
  droplet_size       = var.prod_droplet_size
  droplet_name       = "st0x-issuance-nixos"
  volume_name        = "st0x-issuance-data"
  volume_description = "Persistent storage for SQLite database and logs (issuance)"
}

module "staging" {
  source = "./modules/stack"

  environment  = "staging"
  do_token     = var.do_token
  droplet_size = var.staging_droplet_size
  droplet_name = "st0x-issuance-staging"
  volume_name  = "st0x-issuance-staging-data"
}
