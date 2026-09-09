{
  pkgs,
  lib,
  modulesPath,
  environment,
  volumeName,
  ...
}:

let
  inherit (import ./keys.nix) roles;
  envRoles = roles.${environment};

  # Operator wrapper: defaults to the deployed database and secrets so
  # on-host commands don't require explicit flags.
  issuer = pkgs.writeShellApplication {
    name = "issuer";
    runtimeInputs = [ ];
    text = ''
      export DATABASE_URL="''${DATABASE_URL:-sqlite:///mnt/data/issuance.db}"
      if [ -f /run/agenix/st0x-issuance.env ]; then
        set -a
        # shellcheck source=/dev/null
        . /run/agenix/st0x-issuance.env
        set +a
      fi
      exec /nix/var/nix/profiles/per-service/st0x-issuance/bin/issuer "$@"
    '';
  };
in
{
  imports = [
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
    (modulesPath + "/profiles/qemu-guest.nix")
    ./disko.nix
    ./nix/cloud-init.nix
    ./nix/ingress.nix
    ./nix/upgradeable-services.nix
  ];

  boot.loader.grub = {
    efiSupport = true;
    efiInstallAsRemovable = true;
  };

  networking.useDHCP = lib.mkForce false;

  services = {
    openssh = {
      enable = true;
      openFirewall = true;
      settings = {
        PasswordAuthentication = false;
        PermitRootLogin = "prohibit-password";
        MaxStartups = "50:30:100";
      };
    };

    fail2ban = {
      enable = true;
      bantime = "1h";
      maxretry = 3;
    };
  };

  users = {
    users.root.openssh.authorizedKeys.keys = envRoles.ssh;
    users.st0x = {
      isSystemUser = true;
      group = "st0x";
    };
    groups.st0x = { };
  };

  # The API ports (8000 while Rocket is public, 80/443 for the TLS proxy) are
  # opened by nix/ingress.nix, which owns that switch. SSH is public
  # (openssh.openFirewall above): key-only auth enforced by
  # PasswordAuthentication=false, brute-force noise curbed by fail2ban.
  networking.firewall.enable = true;

  fileSystems."/mnt/data" = {
    device = "/dev/disk/by-id/scsi-0DO_Volume_${volumeName}";
    fsType = "ext4";
  };

  nix = {
    settings = {
      experimental-features = [
        "nix-command"
        "flakes"
      ];
      auto-optimise-store = true;
      download-buffer-size = 268435456;
    };

    gc = {
      automatic = true;
      dates = "weekly";
      options = "--delete-older-than 30d";
    };
  };

  programs.bash.interactiveShellInit = "set -o vi";

  systemd.tmpfiles.rules = [
    "d /mnt/data 0755 st0x st0x -"
    "d /mnt/data/logs 0755 st0x st0x -"
  ];

  # The system bus implementation cannot be live-switched safely. Deploy this
  # change with deploy-rs --boot and a reboot, not a normal switch.
  services.dbus.implementation = "broker";

  environment.systemPackages = with pkgs; [
    bat
    curl
    htop
    magic-wormhole
    sqlite
    rage
    vim
    zellij
    issuer
  ];

  system.stateVersion = "24.11";
}
