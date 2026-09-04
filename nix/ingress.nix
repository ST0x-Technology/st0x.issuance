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
#   true   Rocket listens on 127.0.0.1:8001 and takes the client IP from
#          X-Real-IP, which nginx overwrites from the TCP source on every
#          proxied request. nginx forwards the allowlist below over HTTPS,
#          and (while st0x.ingress.legacyPlaintext is still on) over plaintext
#          8000 as well, so callers can move to the HTTPS name one at a time
#          instead of all at the flip. The whitelists see real client
#          addresses on both.
#
# Forwarding while the app still reads the TCP source would hand every request
# a source of 127.0.0.1, which is inside the default INTERNAL_IP_RANGES: the
# internal routes would degrade to a bare check of a key Alpaca also holds.
# Tying both halves to one option is what rules that state out.
#
# The flip does not require moving callers first: 8000 keeps working through
# nginx afterwards. Retiring plaintext is a later, independent step: set
# st0x.ingress.legacyPlaintext = false and drop the port-8000 rule from infra/
# once nothing calls http://<ip>:8000.
#
# Deploy the flip with `nix run .#<env>DeployAll`, never `.#<env>DeployNixos`.
# The system profile carries the nginx and firewall half; only the service
# profile restarts the app onto the proxied port. A system-only deploy leaves
# nginx proxying to a port nothing is listening on, so the endpoint 502s until
# the service profile follows.
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
  #
  # 8001, not 8000, and that difference is load-bearing. The service unit sets
  # `restartIfChanged = false` (nix/upgradeable-services.nix), so activating
  # the system profile alone rewrites BEHIND_PROXY in the unit file without
  # restarting the running process: nginx would begin proxying while the app
  # still read the TCP source, handing it requests that look like 127.0.0.1 --
  # inside INTERNAL_IP_RANGES, so the two InternalAuth routes below would fall
  # back to a bare check of a key Alpaca also holds. Because the app only
  # binds 8001 once it is actually in proxy mode (see `server_figment` in
  # src/lib.rs), that window fails closed as a 502 instead.
  proxied = {
    proxyPass = "http://127.0.0.1:8001";
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

  # Server level, so scanners walking unrouted paths are limited by the same
  # budget as real callers rather than getting unlimited 403s. Rate is per
  # client address and sized well above real Alpaca volume; a burst is served
  # immediately rather than queued.
  commonLimits = ''
    limit_req zone=issuance_api burst=60 nodelay;
    limit_req_status 429;
    client_max_body_size 64k;
  '';
in
{
  options.st0x.ingress = {
    behindProxy = lib.mkOption {
      type = lib.types.bool;
      default = false;
      description = ''
        Serve the API through the local nginx TLS proxy instead of exposing
        Rocket directly on port 8000. Drives both the proxied routes here and
        BEHIND_PROXY on the service unit; see the header of nix/ingress.nix
        for what each state means and what else has to move in the same
        window.
      '';
    };

    legacyPlaintext = lib.mkOption {
      type = lib.types.bool;
      default = true;
      description = ''
        Keep the plaintext API reachable on port 8000 for callers not yet
        moved to the HTTPS name.

        This is a separate switch from `behindProxy` on purpose. Alpaca
        cannot be pointed at the HTTPS name until HTTPS actually serves,
        which only happens once `behindProxy` is on; if the same switch also
        closed 8000, every caller would break at the flip and stay broken
        until Alpaca shipped their own URL change. With both, the flip brings
        HTTPS up while 8000 keeps working, and dropping plaintext becomes an
        independent later step.

        With `behindProxy` on, nginx serves 8000 itself and forwards the same
        allowlist to the app, so `/admin/*` comes off the public listener at
        the flip rather than waiting for plaintext to go. Turn this off, then
        drop the matching rule in `infra/`, once no caller uses
        `http://<ip>:8000`.
      '';
    };
  };

  config = {
    # behindProxy = false puts the app itself on 8000; closing the port would
    # leave the environment with no reachable API at all.
    assertions = [
      {
        assertion = cfg.behindProxy || cfg.legacyPlaintext;
        message =
          "st0x.ingress: legacyPlaintext can only be disabled once behindProxy "
          + "is enabled, otherwise nothing serves the API.";
      }
    ];

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

      virtualHosts = {
        ${fqdn} = {
          enableACME = true;
          forceSSL = true;
          # 308 keeps the method and body: the mutating routes are POST-only,
          # and a 301 would have clients retry them as a bodiless GET that
          # 404s.
          redirectCode = 308;

          extraConfig = commonLimits;

          locations = if cfg.behindProxy then proxiedRoutes else parkedRoutes;
        };
      }
      # Transitional plaintext listener. Only exists once the app has moved to
      # loopback: before that the app owns 8000 itself and nginx must not try
      # to bind it. Same allowlist and same limits as the HTTPS vhost, and the
      # same X-Real-IP rewrite, so plaintext callers face the IP whitelists
      # exactly as HTTPS callers do.
      // lib.optionalAttrs (cfg.behindProxy && cfg.legacyPlaintext) {
        legacy-plaintext = {
          # Alpaca and the old liquidity droplet address this by IP, so it
          # must answer whatever Host header arrives.
          default = true;
          listen = [
            {
              addr = "0.0.0.0";
              port = 8000;
              ssl = false;
            }
          ];

          extraConfig = commonLimits;
          locations = proxiedRoutes;
        };
      };
    };

    # 80 is ACME http-01 plus the HTTPS redirect; 443 is the API. 8000 is the
    # plaintext path, served by the app before the flip and by nginx after it,
    # until legacyPlaintext is turned off.
    networking.firewall.allowedTCPPorts = [
      80
      443
    ]
    ++ lib.optional cfg.legacyPlaintext 8000;
  };
}
