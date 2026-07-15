variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "prod_droplet_size" {
  description = "Droplet size slug for prod"
  type        = string
  default     = "s-2vcpu-4gb"

  validation {
    condition     = length(trimspace(var.prod_droplet_size)) > 0
    error_message = "prod_droplet_size must not be empty"
  }
}

# s-2vcpu-4gb is sufficient for the issuance server; bump if memory pressure
# is observed during nixos rebuilds or under load.
variable "staging_droplet_size" {
  description = "Droplet size slug for staging"
  type        = string
  default     = "s-1vcpu-2gb"

  validation {
    condition     = length(trimspace(var.staging_droplet_size)) > 0
    error_message = "staging_droplet_size must not be empty"
  }
}
