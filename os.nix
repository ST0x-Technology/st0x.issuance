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
      export FIREBLOCKS_SECRET_PATH="''${FIREBLOCKS_SECRET_PATH:-/run/agenix/fireblocks-secret-issuance.key}"
      if [ -f /run/agenix/st0x-issuance.env ]; then
        # shellcheck disable=SC1091
        set -a; . /run/agenix/st0x-issuance.env; set +a
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
    ./nix/tailscale.nix
    ./nix/upgradeable-services.nix
  ];

  boot.loader.grub = {
    efiSupport = true;
    efiInstallAsRemovable = true;
  };

  networking.useDHCP = lib.mkForce false;

  services = {
    cloud-init = {
      enable = true;
      network.enable = true;
      settings = {
        datasource_list = [
          "ConfigDrive"
          "Digitalocean"
        ];
        datasource.ConfigDrive = { };
        datasource.Digitalocean = { };
        cloud_init_modules = [
          "seed_random"
          "bootcmd"
          "write_files"
          "growpart"
          "resizefs"
          "set_hostname"
          "update_hostname"
          "set_password"
        ];
        cloud_config_modules = [
          "ssh-import-id"
          "keyboard"
          "runcmd"
          "disable_ec2_metadata"
        ];
        cloud_final_modules = [
          "write_files_deferred"
          "puppet"
          "chef"
          "ansible"
          "mcollective"
          "salt_minion"
          "reset_rmc"
          "scripts_per_once"
          "scripts_per_boot"
          "scripts_user"
          "ssh_authkey_fingerprints"
          "keys_to_console"
          "install_hotplug"
          "phone_home"
          "final_message"
        ];
      };
    };

    openssh = {
      enable = true;
      openFirewall = false;
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

  networking.firewall = {
    enable = true;
    # All inbound access is gated by the DO Cloud Firewall (infra/modules/stack)
    # which only permits Tailscale WireGuard. SSH and the issuance API are
    # reached exclusively over tailscale0, which is configured as a trusted
    # interface in tailscale.nix and bypasses the NixOS firewall entirely.
    allowedTCPPorts = [ 443 ];
  };

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
