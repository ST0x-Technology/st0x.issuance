# HTTPS ingress for the public issuance API.
#
# nginx terminates TLS for the environment's FQDN (Let's Encrypt, http-01).
# Whether it forwards anything is controlled by the single option below.
#
# `st0x.ingress.behindProxy` is the one switch for the whole cutover. It sets
# `BEHIND_PROXY` on the service unit (see nix/upgradeable-services.nix) and
# opens the proxied routes here, so the app and nginx cannot disagree:
#
#   false  Rocket listens on 0.0.0.0:8000 and takes the client IP from the TCP
#          source. nginx answers 503 on every path: it exists only to hold the
#          certificate. No route is forwarded, so the IP whitelists keep
#          separating Alpaca from internal callers exactly as they do today.
#
#   true   Rocket listens on 127.0.0.1 and takes the client IP from X-Real-IP,
#          which nginx overwrites from the TCP source on every proxied request.
#          nginx forwards the allowlist below, port 8000 closes, and the
#          whitelists see real client addresses again.
#
# Forwarding while the app still reads the TCP source would hand every request
# a source of 127.0.0.1, which is inside the default INTERNAL_IP_RANGES: the
# internal routes would degrade to a bare check of a key Alpaca also holds.
# Tying both halves to one option is what rules that state out.
#
# Flipping to true requires, in the same window: every caller of
# http://<ip>:8000 moved to the HTTPS name, and the port-8000 firewall rule
# dropped from infra/.
#
# Deploy prerequisites (both are outside this file and neither is enforced by
# the deploy scripts, so a fresh environment must do them first, see
# docs/nixos-provisioning.md):
#   1. An A record for the FQDN at the s01issuer.com registrar (GoDaddy).
#   2. `tf-apply` for the environment, opening TCP 80 and 443 on the
#      DigitalOcean firewall.
# ACME runs during system activation; if http-01 cannot reach port 80 the unit
# fails, activation exits non-zero, and deploy-rs rolls the generation back.
{
  config,
  lib,
  environment,
  ...
}:

let
  cfg = config.st0x.ingress;

  fqdn =
    if environment == "prod" then
      "issuance.s01issuer.com"
    else if environment == "staging" then
      "issuance-staging.s01issuer.com"
    else
      throw "Unsupported environment '${environment}'";

  # X-Real-IP comes from recommendedProxySettings, which appends nixpkgs'
  # proxy-header include to every proxyPass location and sets it to
  # $remote_addr, overwriting whatever the client sent.
  proxied = {
    proxyPass = "http://127.0.0.1:8000";
  };

  # The regexes match the DECODED path: nginx normalizes before location
  # matching, so a symbol or id containing an encoded slash (%2F, which the
  # client crate emits for symbols like "FUND/A") arrives here with a real
  # slash. `.+` keeps those reaching the app instead of falling to the
  # catch-all; the app still resolves the symbol itself.
  proxiedRoutes = {
    # Alpaca ITN (IssuerAuth in the app).
    "= /inkind/issuance" = proxied;
    "= /inkind/issuance/confirm" = proxied;
    "= /accounts/connect" = proxied;

    # GET is Alpaca's asset listing (IssuerAuth); POST on the same path is
    # provisioning (InternalAuth). An nginx location cannot tell them apart,
    # so the method filter is what keeps provisioning off the public listener.
    "= /tokenized-assets" = proxied // {
      extraConfig = ''
        limit_except GET {
          deny all;
        }
      '';
    };

    # Liquidity bot (InternalAuth in the app): freeze-status reads and
    # mint-authorization delivery.
    "~ ^/tokenized-assets/.+/status$" = proxied;
    "~ ^/internal/mints/.+/authorization$" = proxied;

    # Everything else (admin, provisioning, docs) is not served here.
    "/".return = "403";
  };

  # Nothing is forwarded until the app trusts the proxy.
  parkedRoutes = {
    "/".return = "503";
  };
in
{
  options.st0x.ingress.behindProxy = lib.mkOption {
    type = lib.types.bool;
    default = false;
    description = ''
      Serve the API through the local nginx TLS proxy instead of exposing
      Rocket directly on port 8000. Drives both the proxied routes here and
      BEHIND_PROXY on the service unit; see the header of nix/ingress.nix for
      what each state means and what else has to move in the same window.
    '';
  };

  config = {
    security.acme = {
      acceptTerms = true;
      defaults.email = "kais@rainlang.xyz";
    };

    services.nginx = {
      enable = true;
      recommendedTlsSettings = true;
      recommendedProxySettings = true;

      appendHttpConfig = ''
        limit_req_zone $binary_remote_addr zone=issuance_api:10m rate=30r/s;
      '';

      virtualHosts.${fqdn} = {
        enableACME = true;
        forceSSL = true;
        # 308 keeps the method and body: the mutating routes are POST-only, and
        # a 301 would have clients retry them as a bodiless GET that 404s.
        redirectCode = 308;

        # Server level, so scanners walking unrouted paths are limited by the
        # same budget as real callers rather than getting unlimited 403s.
        # Rate is per client address and sized well above real Alpaca volume;
        # a burst is served immediately rather than queued.
        extraConfig = ''
          limit_req zone=issuance_api burst=60 nodelay;
          limit_req_status 429;
          client_max_body_size 64k;
        '';

        locations = if cfg.behindProxy then proxiedRoutes else parkedRoutes;
      };
    };

    # 80 is ACME http-01 plus the HTTPS redirect; 443 is the API. Port 8000 is
    # only worth opening while Rocket still listens on a public address.
    networking.firewall.allowedTCPPorts = [
      80
      443
    ]
    ++ lib.optional (!cfg.behindProxy) 8000;
  };
}
