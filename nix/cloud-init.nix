# Cloud-init stack shared by bootstrap.nix and os.nix so the module lists
# cannot drift between the two configs.
#
# The module lists are curated; critically, cloud-init's `ssh` module
# (cc_ssh) must never run. Its ssh_deletekeys behavior (upstream default:
# true) deletes and regenerates all /etc/ssh/ssh_host_* keys on first boot,
# which discards the host keys nixos-anywhere installs via --copy-host-keys
# and trips the pre/post-reboot host-key comparison scripts/bootstrap.nu
# uses for MITM detection. The nixpkgs default cloud_config_modules include
# `ssh`, so inheriting the defaults (as bootstrap.nix once did) bricks the
# bootstrap. ssh_deletekeys = false guards the same failure mode should the
# module ever be re-added to a list.
_:

{
  services.cloud-init = {
    enable = true;
    network.enable = true;
    settings = {
      datasource_list = [
        "ConfigDrive"
        "Digitalocean"
      ];
      datasource.ConfigDrive = { };
      datasource.Digitalocean = { };
      ssh_deletekeys = false;
      cloud_init_modules = [
        "seed_random"
        "bootcmd"
        "write_files"
        "growpart"
        "resizefs"
        "set_hostname"
        "update_hostname"
        "set_passwords"
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
}
