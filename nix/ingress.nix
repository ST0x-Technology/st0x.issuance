# HTTPS ingress for the public issuance API (Linear RAI-236).
#
# nginx terminates TLS for the environment's FQDN (Let's Encrypt, http-01)
# and proxies an explicit allowlist of routes to Rocket on 127.0.0.1:8000:
# the four Alpaca-facing routes plus the two routes the liquidity bot calls.
# Admin and provisioning routes are deliberately not proxied; they stay
# reachable from loopback only (SSH tunnel), so a leaked key cannot use them
# from the network.
#
# Pairing with the app: nginx sets X-Real-IP from the TCP source address on
# every proxied request, overwriting anything the client sent. The app only
# trusts that header when BEHIND_PROXY=true, which also rebinds Rocket to
# loopback so nginx is the only way in. Until BEHIND_PROXY flips, Rocket
# stays on 0.0.0.0:8000 and proxied requests reach it from 127.0.0.1, which
# passes the default INTERNAL_IP_RANGES but fails a configured
# ALPACA_IP_RANGES: flip Alpaca traffic to HTTPS and BEHIND_PROXY together.
#
# DNS for the FQDNs lives at the s01issuer.com registrar (GoDaddy): A records
# pointing at each environment's DO reserved IP.
{ environment, ... }:

let
  fqdn =
    {
      prod = "issuance.s01issuer.com";
      staging = "issuance-staging.s01issuer.com";
    }
    .${environment} or (throw "Unsupported environment '${environment}'");

  proxied = {
    proxyPass = "http://127.0.0.1:8000";
    extraConfig = ''
      proxy_set_header X-Real-IP $remote_addr;
      limit_req zone=issuance_api burst=20 nodelay;
    '';
  };
in
{
  security.acme = {
    acceptTerms = true;
    defaults.email = "kais@rainlang.xyz";
  };

  services.nginx = {
    enable = true;
    recommendedTlsSettings = true;
    recommendedProxySettings = true;

    # Per-IP rate limit on the public listener: generous next to real Alpaca
    # call rates, hostile to scanners. Applied per proxied location.
    appendHttpConfig = ''
      limit_req_zone $binary_remote_addr zone=issuance_api:10m rate=10r/s;
    '';

    virtualHosts.${fqdn} = {
      enableACME = true;
      forceSSL = true;

      locations = {
        # Alpaca ITN (IssuerAuth in the app).
        "= /inkind/issuance" = proxied;
        "= /inkind/issuance/confirm" = proxied;
        "= /accounts/connect" = proxied;
        "= /tokenized-assets" = proxied;

        # Liquidity bot (InternalAuth in the app): freeze-status reads and
        # mint-authorization delivery.
        "~ ^/tokenized-assets/[^/]+/status$" = proxied;
        "~ ^/internal/mints/[^/]+/authorization$" = proxied;

        # Everything else (admin, provisioning, docs) is not served here.
        "/".extraConfig = "return 403;";
      };
    };
  };

  # 80 is ACME http-01 + the forceSSL redirect; 443 is the API. Merges with
  # the existing allowedTCPPorts in os.nix.
  networking.firewall.allowedTCPPorts = [
    80
    443
  ];
}
