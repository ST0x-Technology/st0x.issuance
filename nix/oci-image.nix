# OCI image for the GCP staging/production VMs (s01.devops
# terraform/staging-issuance). Built by the flake (dockerTools, no
# Dockerfile, no base image); `created` is pinned so the same commit
# rebuilds to the same digest. Pushed + attested to the s01-issuance
# Artifact Registry repo in s01-artifacts by
# .github/workflows/build-oci.yml (main only).
#
# Image contract (consumed by the stack's docker-compose; keep in sync):
#   Entrypoint = the `st0x-issuance` binary. Configuration arrives entirely
#   through the environment, exactly like the systemd unit it replaces
#   (nix/upgradeable-services.nix): DATABASE_URL, ENVIRONMENT, LOG_LEVEL,
#   BEHIND_PROXY, CONFIG=<path to a mounted TOML>, plus the secret env vars
#   from the compose env_file (Secret Manager on the VM, agenix on the
#   droplet). Nothing is baked: no config file, no secrets.
#   database_url stays sqlite:///mnt/data/issuance.db (the VM mounts its
#   data disk there, byte-identical to the droplet).
#   Rocket listens on 8000 (direct mode; BEHIND_PROXY moves it behind a
#   proxy on the droplet, which the VM does not run).
{
  pkgs,
  st0x-issuance,
}:
{
  bot-oci = pkgs.dockerTools.streamLayeredImage {
    name = "s01-issuance";
    tag = "latest";
    created = "1970-01-01T00:00:01Z";
    contents = [
      st0x-issuance
      pkgs.cacert
    ];
    config = {
      Entrypoint = [ "${st0x-issuance}/bin/st0x-issuance" ];
      Env = [
        "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
      ];
      ExposedPorts = {
        "8000/tcp" = { };
      };
    };
  };
}
