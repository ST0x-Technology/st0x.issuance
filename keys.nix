rec {
  keys = {
    # purpose: dev/ops
    gleb = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHepyxN9hvXzbCY/z0amzldy7DXjNdyetnVaQexRgDEX";
    # purpose: dev/ops
    juan = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHHNeV3nfiJS0QE2JoW3d0dRw1j6OVKl7rXor24XHvsd";
    # purpose: op sec auditing and manual cli while fixing gaps in automation
    alastair = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJArH3PA+bFIon0JkCVQGs9aWr45lnVjiiTLLO9BPItn";

    # purpose: deployments from github actions
    ci = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOkrujoBNXILB8Ypv90puKKhmKq161gitlvUFM5n0c+d";
    # purpose: initial key for Digital Ocean to include when first creating
    # the instance and it's matched by name in their dashboard
    st0x-op = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPZ56nOYbGDd0ZfbqxeY7AbvaQGQrHnlC80ccpRGpCoj";

    # purpose: used by the remote host to decrypt ragenix secrets we need for
    # the system to work at all. auto-generated during bootstrap when
    # nixos-anywhere turns the initially ubuntu instance into nixos.
    # Set after provisioning the infra running `nix run .#bootstrap -- prod`.
    host-prod = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOg0aUOHOUB39qgMvT5Z66yVl/FGfN808X0Tla3WGfvo";
    # Set after provisioning the infra running `nix run .#bootstrap -- staging`.
    host-staging = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIARxfgHmttVQAyhhqwIZG4UAM/ZjuShUkCCyN2h9Ma/H";
  };

  roles = with keys; {
    # access to terraform state and encrypted vars (shared across environments)
    infra = [
      st0x-op
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
