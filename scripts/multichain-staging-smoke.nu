#!/usr/bin/env nu
# Preflight and registration helpers for multichain staging validation (RAI-1210).
# See docs/runbooks/multichain-staging-validation.md.

def base-url []: nothing -> string {
    $env.ISSUER_BASE_URL? | default "http://localhost:8000"
}

def api-key []: nothing -> string {
    let key = $env.ISSUER_API_KEY?
    if $key == null {
        error make {msg: "set ISSUER_API_KEY"}
    }
    $key
}

def staging-underlying []: nothing -> string {
    $env.STAGING_UNDERLYING? | default "TSLA"
}

def staging-token []: nothing -> string {
    $env.STAGING_TOKEN? | default "tTSLA"
}

def auth-headers []: nothing -> list<string> {
    [X-API-KEY (api-key)]
}

def fetch-status [url: string]: nothing -> int {
    (
        http get --headers (auth-headers) --full --allow-errors
            --max-time 10sec $url
    ).status
}

# HTTP checks for the ?network= contract.
def "main preflight" [] {
    let base = (base-url)
    let underlying = (staging-underlying)
    print $"== Preflight: ($base) =="

    let status_base = (
        fetch-status $"($base)/tokenized-assets/($underlying)/status?network=base"
    )
    print $"GET .../status?network=base -> ($status_base) \(expect 200 or 404\)"
    if $status_base not-in [200 404] {
        error make {msg: "unexpected status for base network query"}
    }

    let status_missing = (
        fetch-status $"($base)/tokenized-assets/($underlying)/status"
    )
    print $"GET .../status \(no ?network=\) -> ($status_missing) \(expect 422\)"
    if $status_missing != 422 {
        error make {msg: "missing ?network= must return 422"}
    }

    let status_eth = (
        fetch-status $"($base)/tokenized-assets/($underlying)/status?network=ethereum"
    )
    print $"GET .../status?network=ethereum -> ($status_eth) \(expect 200 or 404\)"
    if $status_eth not-in [200 404] {
        error make {msg: "unexpected status for ethereum network query"}
    }

    print "Preflight OK"
}

# POST the chain B vault (needs STAGING_ETHEREUM_VAULT). GETs first and skips
# POST when token/vault/network already match; aborts on mismatch.
def "main register-ethereum-asset" [] {
    let vault = $env.STAGING_ETHEREUM_VAULT?
    if $vault == null {
        error make {msg: "set STAGING_ETHEREUM_VAULT to the chain B vault address"}
    }

    let base = (base-url)
    let underlying = (staging-underlying)
    let token = (staging-token)
    print $"== Register ($underlying) on ethereum =="

    let detail_url = $"($base)/tokenized-assets/($underlying)?network=ethereum"
    let existing = (
        http get --headers (auth-headers) --full --allow-errors
            --max-time 10sec $detail_url
    )

    if $existing.status == 200 {
        let body = $existing.body
        let registered_vault = ($body.vault? | default "" | str downcase)
        let registered_token = ($body.token? | default "")
        let registered_network = ($body.network? | default "")

        if $registered_vault != ($vault | str downcase) {
            error make {
                msg: $"ethereum asset already registered with vault ($registered_vault), refusing ($vault)"
            }
        }
        if $registered_token != $token {
            error make {
                msg: $"ethereum asset already registered with token ($registered_token), refusing ($token)"
            }
        }
        if $registered_network != "ethereum" {
            error make {
                msg: $"ethereum asset has unexpected network ($registered_network)"
            }
        }

        print "Asset already registered with matching token/vault/network; skipping POST"
        return
    }

    if $existing.status != 404 {
        error make {
            msg: $"unexpected GET before register: ($existing.status) ($existing.body)"
        }
    }

    let response = (
        http post
            --content-type application/json
            --headers (auth-headers)
            --full --allow-errors
            --max-time 10sec
            $"($base)/tokenized-assets"
            {
                underlying: $underlying,
                token: $token,
                network: "ethereum",
                vault: $vault,
            }
    )
    print $"POST /tokenized-assets -> ($response.status) \(expect 201\)"
    if $response.status != 201 {
        error make {
            msg: $"asset registration failed: ($response.status) ($response.body)"
        }
    }
}

# GET the internal asset detail with ?network=ethereum (expect 200) and
# confirm the registered vault matches STAGING_ETHEREUM_VAULT.
def "main verify-ethereum-asset" [] {
    let vault = $env.STAGING_ETHEREUM_VAULT?
    if $vault == null {
        error make {msg: "set STAGING_ETHEREUM_VAULT to the chain B vault address"}
    }

    let base = (base-url)
    let underlying = (staging-underlying)
    print "== Verify chain B asset registered =="

    let response = (
        http get --headers (auth-headers) --full --allow-errors
            --max-time 10sec
            $"($base)/tokenized-assets/($underlying)?network=ethereum"
    )
    print $"GET .../($underlying)?network=ethereum -> ($response.status) \(expect 200\)"
    if $response.status != 200 {
        error make {
            msg: $"ethereum asset detail not found: ($response.status) ($response.body)"
        }
    }

    let registered_vault = ($response.body.vault? | default "" | str downcase)
    if $registered_vault != ($vault | str downcase) {
        error make {
            msg: $"registered vault ($registered_vault) does not match STAGING_ETHEREUM_VAULT ($vault)"
        }
    }

    let registered_token = ($response.body.token? | default "")
    if $registered_token != (staging-token) {
        error make {
            msg: $"registered token ($registered_token) does not match STAGING_TOKEN ((staging-token))"
        }
    }

    print ($response.body | to json)
}

# GET the ITN token list -- Alpaca auth, run from an Alpaca-whitelisted IP.
def "main verify-token-list" [] {
    let base = (base-url)
    let underlying = (staging-underlying)
    let token = (staging-token)
    print "== Verify ITN token list (Alpaca auth -- run from Alpaca IP range) =="

    let response = (
        http get --headers (auth-headers) --full --allow-errors
            --max-time 10sec
            $"($base)/tokenized-assets"
    )
    print $"GET /tokenized-assets -> ($response.status) \(expect 200\)"
    if $response.status != 200 {
        error make {
            msg: $"token list request failed: ($response.status) ($response.body)"
        }
    }

    let body = $response.body
    print ($body | to json)

    let matching_rows = (
        $body.tokens?
        | default []
        | where underlying == $underlying and token == $token
    )
    if ($matching_rows | length) != 1 {
        error make {
            msg: $"expected exactly 1 token-list row for ($underlying)/($token), found ($matching_rows | length)"
        }
    }

    let networks = (
        $matching_rows
        | first
        | get networks?
        | default []
    )
    if "ethereum" not-in $networks {
        error make {
            msg: $"token list row for ($underlying)/($token) missing network ethereum"
        }
    }

    print $"Token list includes ethereum for ($underlying)/($token)"
}

def main [] {
    print -e "Usage: multichain-staging-smoke.nu <command>

Commands:
  preflight                 HTTP checks for the ?network= contract
  register-ethereum-asset   POST chain B vault after GET pre-check (needs
                            STAGING_ETHEREUM_VAULT)
  verify-ethereum-asset     GET internal detail ?network=ethereum and check
                            token and vault (needs STAGING_ETHEREUM_VAULT)
  verify-token-list         GET ITN list -- Alpaca IP whitelist only

Environment:
  ISSUER_BASE_URL           default http://localhost:8000
  ISSUER_API_KEY            required
  STAGING_UNDERLYING        default TSLA
  STAGING_TOKEN             default tTSLA
  STAGING_ETHEREUM_VAULT    required for register/verify-ethereum-asset"
    exit 1
}
