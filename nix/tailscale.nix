# Tailscale stack for deployed issuance hosts: enrolls the node onto
# our tailnet via a per-environment agenix-encrypted auth key, opens
# the WireGuard port, and marks `tailscale0` as a trusted firewall
# interface so SSH/admin traffic can remain private.
#
# Unlike the liquidity host there is no nginx / TLS cert step here —
# the issuance API preserves the existing public plain-HTTP Alpaca endpoint
# on port 8000, while operators use the same service over the tailnet.
#
# `tailscaled.ExecStartPre` deletes the stale `tailscale0` TUN device
# so the unit doesn't crash-loop when a previous tailscaled hasn't
# released it yet.
{ pkgs, environment, ... }:

{
  # Per-environment reusable, tagged auth key. Used only on first
  # enrollment — after that Tailscale re-authenticates via the stored
  # node key in /var/lib/tailscale. To rotate the node identity
  # (e.g. re-tag), run `tailscale up --force-reauth --auth-key ...`
  # manually on the droplet.
  services.tailscale = {
    enable = true;
    authKeyFile = "/run/agenix/tailscale-authkey-${environment}";
  };

  networking.firewall = {
    allowedUDPPorts = [
      41641 # Tailscale WireGuard
    ];
    trustedInterfaces = [ "tailscale0" ];
  };

  age.secrets."tailscale-authkey-${environment}" = {
    file = ../secret/tailscale-authkey-${environment}.age;
    mode = "0400";
  };

  # Clean up stale TUN device before tailscaled starts. During NixOS
  # activation the old tailscaled may still hold /dev/net/tun when the
  # new unit starts, causing a crash-loop.
  systemd.services.tailscaled.serviceConfig.ExecStartPre = [
    "-${pkgs.iproute2}/bin/ip link delete tailscale0"
  ];
}
