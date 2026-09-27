terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = ">= 2.40"
    }
  }
}

data "digitalocean_ssh_key" "deploy" {
  name = var.ssh_key_name
}

locals {
  droplet_name = coalesce(var.droplet_name, "st0x-issuance-${var.environment}")
  volume_name  = coalesce(var.volume_name, "st0x-issuance-${var.environment}-data")
}

resource "digitalocean_volume" "data" {
  region                  = var.region
  name                    = local.volume_name
  size                    = var.volume_size_gb
  initial_filesystem_type = "ext4"
  description             = coalesce(var.volume_description, "Persistent storage for SQLite database and logs (${var.environment})")

  lifecycle {
    prevent_destroy = true
  }
}

resource "digitalocean_droplet" "nixos" {
  image    = "ubuntu-24-04-x64"
  name     = local.droplet_name
  region   = var.region
  size     = var.droplet_size
  ssh_keys = [data.digitalocean_ssh_key.deploy.id]
}

resource "digitalocean_volume_attachment" "data" {
  droplet_id = digitalocean_droplet.nixos.id
  volume_id  = digitalocean_volume.data.id
}

resource "digitalocean_reserved_ip" "nixos" {
  region = var.region
}

resource "digitalocean_reserved_ip_assignment" "nixos" {
  ip_address = digitalocean_reserved_ip.nixos.ip_address
  droplet_id = digitalocean_droplet.nixos.id
}

resource "digitalocean_firewall" "st0x_issuance" {
  name        = "st0x-issuance-${var.environment}"
  droplet_ids = [digitalocean_droplet.nixos.id]

  # Public SSH for bootstrap (Ubuntu image + nixos-anywhere), operators, and
  # CI deploys. Key-only auth (PasswordAuthentication=false) plus fail2ban are
  # enforced by os.nix on the host.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # Legacy plaintext issuance API (RAI-236). Kept while Alpaca and the old
  # liquidity droplet still call http://<ip>:8000 directly; tighten
  # api_source_addresses to those callers' ranges, then drop this rule once
  # both are on the HTTPS endpoint. App-level auth still enforces X-API-KEY
  # and the IP whitelists.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "8000"
    source_addresses = var.api_source_addresses
  }

  # ACME http-01 challenge + redirect-to-HTTPS (nginx, nix/ingress.nix).
  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # HTTPS issuance API (nginx, nix/ingress.nix): TLS in front of an explicit
  # route allowlist. Tighten to Alpaca CIDRs + liquidity egress IPs once
  # Alpaca provides theirs.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = var.https_source_addresses
  }

  # All outbound
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
