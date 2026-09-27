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

# Firewall source ranges for the issuance API, per environment (RAI-236).
#
# These exist at the root so tightening a range is a terraform.tfvars edit
# rather than a module change. A value set in tfvars for a variable that is
# not declared here is only a warning, not an error, so leaving them out
# would let an operator "tighten" a port and get a silent no-op while it
# stayed open to the world.
variable "prod_api_source_addresses" {
  description = "Source ranges allowed to reach prod's legacy plaintext API on port 8000. Tighten to Alpaca ranges + the old liquidity droplet, then drop the rule after the HTTPS cutover."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]

  validation {
    condition     = length(var.prod_api_source_addresses) > 0
    error_message = "prod_api_source_addresses must not be empty; an empty list silently drops the rule instead of restricting it"
  }
}

variable "prod_https_source_addresses" {
  description = "Source ranges allowed to reach prod's HTTPS API on port 443. World until Alpaca provides egress CIDRs."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]

  validation {
    condition     = length(var.prod_https_source_addresses) > 0
    error_message = "prod_https_source_addresses must not be empty; an empty list silently drops the rule instead of restricting it"
  }
}

variable "staging_api_source_addresses" {
  description = "Source ranges allowed to reach staging's legacy plaintext API on port 8000."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]

  validation {
    condition     = length(var.staging_api_source_addresses) > 0
    error_message = "staging_api_source_addresses must not be empty; an empty list silently drops the rule instead of restricting it"
  }
}

variable "staging_https_source_addresses" {
  description = "Source ranges allowed to reach staging's HTTPS API on port 443."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]

  validation {
    condition     = length(var.staging_https_source_addresses) > 0
    error_message = "staging_https_source_addresses must not be empty; an empty list silently drops the rule instead of restricting it"
  }
}
