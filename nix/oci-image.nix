# OCI image for the GCP staging/production VMs (s01.devops
# terraform/staging-issuance). Built by the flake (dockerTools, no
# Dockerfile, no base image); `created` is pinned so the same commit
# rebuilds to the same digest. Pushed + attested to the s01-issuance
# Artifact Registry repo in s01-artifacts by
# .github/workflows/build-oci.yml (main only).
#
# Image contract (consumed by the stack's docker-compose; keep in sync):
#   Entrypoint = the `st0x-issuance` binary. ALL configuration is
#   environment variables, exactly like the systemd unit it replaces
#   (nix/upgradeable-services.nix `staticEnvironment` + its agenix
#   EnvironmentFile). REQUIRED compose variables: DATABASE_URL,
#   ENVIRONMENT (staging|production: the binary defaults to production,
#   so an omitted value makes a staging box present as prod; the compose
#   must set it explicitly, never rely on the default), LOG_LEVEL and
#   CONFIG=<path to a mounted TOML>; plus the secret variables
#   (RPC_URL, signer, Alpaca, notifications: see src/config.rs `env =`)
#   through a compose env_file. There is no secrets-file loader in the
#   app; a mounted secrets TOML is never read. Nothing is baked: no config
#   file, no secrets.
#   database_url stays sqlite:///mnt/data/issuance.db (the VM mounts its
#   data disk there, byte-identical to the droplet).
#   Rocket listens on 0.0.0.0:8000, fixed in build_rocket (src/lib.rs);
#   there is no proxy mode or port knob on main.
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
