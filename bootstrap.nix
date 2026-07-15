{
  lib,
  modulesPath,
  environment,
  ...
}:

let
  inherit (import ./keys.nix) roles;
  envRoles = roles.${environment};

in
{
  imports = [
    (modulesPath + "/virtualisation/digital-ocean-config.nix")
    (modulesPath + "/profiles/qemu-guest.nix")
    ./disko.nix
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
      };
    };

    openssh = {
      enable = true;
      settings = {
        PasswordAuthentication = false;
        PermitRootLogin = "prohibit-password";
      };
    };
  };

  users.users.root.openssh.authorizedKeys.keys = envRoles.ssh;

  networking.firewall = {
    enable = true;
    allowedTCPPorts = [ 22 ];
  };

  nix.settings.experimental-features = [
    "nix-command"
    "flakes"
  ];

  # Must match os.nix. The broker implementation cannot be live-switched, so
  # it must be present from the first boot (installed by nixos-anywhere) so
  # the initial switch-to-configuration from prodDeployAll/stagingDeployAll
  # finds it already active and does not attempt to change it.
  services.dbus.implementation = "broker";

  system.stateVersion = "24.11";
}
