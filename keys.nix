rec {
  # Tailscale MagicDNS host labels per environment. Used by infra/default.nix
  # (local tooling) and the deploy workflows to resolve the deploy target.
  # The full MagicDNS name is "<label>.<tailnet-suffix>.ts.net" — update
  # tailnetSuffix below to match your Tailscale account if it differs.
  tailnetSuffix = "taile5cf8a.ts.net";
  tailscaleHost = {
    prod = "st0x-issuance-nixos";
    staging = "st0x-issuance-staging";
  };

  keys = {
    # purpose: dev/ops
    gleb = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    # purpose: dev/ops
    juan = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHHNeV3nfiJS0QE2JoW3d0dRw1j6OVKl7rXor24XHvsd";
    # purpose: op sec auditing and manual cli while fixing gaps in automation
    alastair = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJArH3PA+bFIon0JkCVQGs9aWr45lnVjiiTLLO9BPItn";

    # purpose: deployments from github actions
    ci = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARWUchVuQvkFm2tzspdC79hhRyYbWzRjs5iimhxewUy";
    # purpose: initial key for Digital Ocean to include when first creating
    # the instance and it's matched by name in their dashboard
    st0x-op = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPZ56nOYbGDd0ZfbqxeY7AbvaQGQrHnlC80ccpRGpCoj";

    # purpose: used by the remote host to decrypt ragenix secrets we need for
    # the system to work at all. auto-generated during bootstrap when
    # nixos-anywhere turns the initially ubuntu instance into nixos.
    # PLACEHOLDER: replace with real key after `nix run .#prodBootstrap` (see BOOTSTRAP.md).
    host-prod = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    # PLACEHOLDER: replace with real key after `nix run .#stagingBootstrap`.
    host-staging = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  };

  roles = with keys; {
    # access to terraform state and encrypted vars (shared across environments)
    infra = [
      st0x-op
      ci
    ];

    prod = {
      ssh = [
        juan
        gleb
        alastair
        st0x-op
        ci
      ];
      service = [
        st0x-op
        host-prod
      ];
    };

    staging = {
      ssh = [
        gleb
        juan
        st0x-op
        ci
      ];
      service = [
        st0x-op
        host-staging
      ];
    };
  };
}
