# Robinhood Chain vault registration runbook (RAI-2284)

Registers all 56 Robinhood Chain (4663) tokenized assets with the issuer via
`POST /tokenized-assets`, after the `CHAIN_ROBINHOOD_*` secrets and the
Robinhood deploy config are live. Registration is the step that makes the
Robinhood runtime mandatory on every subsequent start, so it is the last step,
not the first.

This runbook does not enable the orchestrator. Every asset stays vault-direct
([`config.prod.toml`](../../config.prod.toml) pins
`default_vault_mode = "vault_direct"`); the `[orchestrator.addresses]` entry
recorded there is read only by the onboarding tooling.

## Ordering

The three pieces must land in this order: step 2 aborts startup without step 1,
and step 3 fails closed without both.

1. **`CHAIN_ROBINHOOD_*` in the deployment secrets**
   (`secret/st0x-issuance-prod.env.age` and
   `secret/st0x-issuance-staging.env.age` in this repository).
   `CHAIN_ROBINHOOD_RPC_URL`, `CHAIN_ROBINHOOD_CHAIN_ID` and
   `CHAIN_ROBINHOOD_BACKFILL_START_BLOCK` are an all-or-nothing group, and the
   chain id is checked against Robinhood Chain's canonical 4663.
   `CHAIN_ROBINHOOD_LOW_GAS_THRESHOLD` is optional and may be set alongside
   them.
2. **The Robinhood deploy config** (this repo, `config.prod.toml` /
   `config.staging.toml`). `[wrapped_tokens.robinhood]` is rejected at startup
   with `WrappedTokensForUnconfiguredNetwork` while Robinhood has no chain
   configuration, so rolling this config out ahead of the secrets takes the
   service down. Roll the secrets first, or roll both in one restart.
3. **The registrations below.** `POST /tokenized-assets` returns 422 for a
   network with no chain configuration, so an early attempt fails closed without
   writing an event.

## Preconditions

- The service restarted cleanly with both the secrets and the config in place.
  The startup log must show a wrapped-transfer watcher spawned for Robinhood,
  not `"No wrapped tokens configured"` (`spawn_wrapped_transfer_monitors`).
- No configured wrapper address is also an enabled asset's vault on Robinhood:
  the watcher refuses such a token outright, since every redemption transfer
  would page as an un-redeemable inbound transfer. The wrapper and vault address
  sets are disjoint as listed here, so this holds by construction unless a
  registration below uses the wrong address.
- The issuer signer is funded with Robinhood Chain gas (ETH — Robinhood Chain is
  an Arbitrum Orbit L2) above `CHAIN_ROBINHOOD_LOW_GAS_THRESHOLD`.
- A database backup is recorded. Registration appends events immediately and is
  not undone by a config rollback.
- `ISSUER_BASE_URL` and `ISSUER_API_KEY` are exported, and the call is made from
  an allowlisted host.

## Request shape

`AddTokenizedAssetRequest` (`crates/dto/src/lib.rs`) is four fields:

| Field        | Value                                                               |
| ------------ | ------------------------------------------------------------------- |
| `underlying` | the underlying symbol, e.g. `MSTR`                                  |
| `token`      | the **token symbol**, `t<UNDERLYING>` — a symbol, not an address    |
| `network`    | `robinhood`                                                         |
| `vault`      | the `OffchainAssetReceiptVault` address (the `t<UNDERLYING>` ERC20) |

`token` is a `TokenSymbol` string, so the wrapper (`wt<UNDERLYING>`) address
never appears in a registration. Wrappers are configured separately, under
`[wrapped_tokens.robinhood]`, and are watched rather than issued against.

`vault` is the receipt **vault**, not the ERC-1155 receipt contract that sits
beside it: on Robinhood the vault answers `symbol() == "t<UNDERLYING>"` while
the receipt answers `"t<UNDERLYING> RCPT"` and reports the ERC-1155 interface.
Registering the receipt would point mints at the wrong contract.

## The 56 registrations

Symbol and receipt-vault address. Every address below was read back on-chain
against `https://rpc.mainnet.chain.robinhood.com` and returned
`symbol() == "t<UNDERLYING>"`.

Six are EU listings whose underlying contains a dot (`AIR.PA`, `BMW.DE`,
`MC.PA`, `SIE.DE`, `MBG.DE`, `RHM.DE`). The dot is part of the symbol on-chain
and in the JSON body — do not strip or substitute it. It only needs special
handling in the TOML config, where the key must be quoted; JSON has no such
restriction.

```bash
register_all() {
  register "AAPL" "0xC982730643321f3643436Eb4a6E910219Caa55f0" || return 1
  register "AIR.PA" "0x4Da175A70020EeEFa71C36d710307ed0803Aed60" || return 1
  register "AMAT" "0x26139195f7a82a52c62C4047F843C99c61Ed9a5E" || return 1
  register "AMD" "0xe2De878d8Ca6bA544BFE80a10fAb0395DF32Cff0" || return 1
  register "AMZN" "0x6615f3D82989949fa7d167b40FEc0Ef30cdbA476" || return 1
  register "ARKK" "0xDf406836D5A092894ee5d5bdC7F58e5bdc8D196A" || return 1
  register "ASML" "0x77Ee94C8B85cF48a426F84B0F2d796eC77e41c7b" || return 1
  register "AVGO" "0x503a69b2918DFA152eb0e0803174ed90ba5be756" || return 1
  register "BABA" "0xb3f9A61b0e97c7F7Bb85Ea5E9Dad0da8f3496B57" || return 1
  register "BMNR" "0x00472aA0D0611F933c22b8148F02B0cDd1Ae5fbc" || return 1
  register "BMW.DE" "0x6872cEe8E1a07Dd8C1154755f7e215F2973caE52" || return 1
  register "CBRS" "0x75E0d127794b9C26eE35c55fbaBcc41c53Ccb37C" || return 1
  register "CEG" "0x6240E93f1A08d43002e52b210cD49c54C813D780" || return 1
  register "COIN" "0x5100ED387Ab3ED37667199aD8f9F6D963157d28e" || return 1
  register "CRCL" "0xeF63EdB9F39Dcd03C103A11e2e7E1878308b9586" || return 1
  register "DNUT" "0x4a88c84AA04a5151997e8E503BEe7fD92E0918A9" || return 1
  register "DRAM" "0x5F5a1Bdc00ade7702ea5944B1374DC598dA3759f" || return 1
  register "FGI" "0xfEa217600e2b00bBB2172a99345016a0cEb7d29f" || return 1
  register "FTF" "0x334ccaD2e7D774F5e6A13437977dD0878926deF8" || return 1
  register "GOOGL" "0xDA52106FC0D44096Fd500E096b9045FdAc1d27B9" || return 1
  register "GRND" "0xdca06fddf5320870C8E9D0534aa102677C36bCc4" || return 1
  register "HOOD" "0x50DE74136b67911799fc39B726bFC2707cCec769" || return 1
  register "IAU" "0xb9A1D1822F57f52959b8c5097A8322D534bceDEe" || return 1
  register "IBHG" "0x36b30F5B5D1AcD3D8135Afe7a5516A300021f139" || return 1
  register "INTC" "0xb526Bf49DAB7F72B772FEF4B6D572C254A454ef7" || return 1
  register "LLY" "0x15d415952a36D4cE80671e918B2531bdB25274E5" || return 1
  register "LRCX" "0xe68A46547CdBBB181587B32cCD1A505Bedf0a994" || return 1
  register "MBG.DE" "0xA1768baE756058fE00dD281C405DDe1C48B00F3B" || return 1
  register "MC.PA" "0xaE7115d434c84F2f4a1196BaD67415d479804af1" || return 1
  register "MCD" "0xC04160F3e18e120C2259f3FE33864823bF3b9015" || return 1
  register "MSFT" "0x3b3936b5Ec170Cdb5823012dBF4dF1d56Cfa1ba5" || return 1
  register "MSTR" "0x8500189061e2206Bc33Bf04DC10fFB1Fe7dED637" || return 1
  register "MU" "0x2C845ed32c5fDE012eb14508d0d24BD3300B1D71" || return 1
  register "NKE" "0x5e6e803242E52451FfdA82Fd7b5Ce4967B95C76E" || return 1
  register "NVDA" "0xf6A89b0c9FF897000E37bBD06397992278FfC50d" || return 1
  register "ORCL" "0x06aE3f6CFaE124039902a79Da44ad2a4A4489250" || return 1
  register "PLBY" "0x4a18036Dce22168D8891919a1c75aC2CAf9a08AB" || return 1
  register "PPLT" "0x47C2e6644eFDF58E86dA45dC60e0f67A65043B99" || return 1
  register "PTY" "0xf3875383506677BCdA6b9F12c48Ff7fE300970D7" || return 1
  register "QQQM" "0x5Aa65dfF455C7f18C21370086EaFaEe4f2b63608" || return 1
  register "RHM.DE" "0x8790337c4Ce51b66CBa179129d830A3683780ff6" || return 1
  register "RKLB" "0xED0c085d92C262FB46937CB0B3C9763Af7fCCf30" || return 1
  register "SGOV" "0x344147366F648640076d363FAF659c214788E99d" || return 1
  register "SIE.DE" "0x269CA594b6463F0D94086fb2D77dFaad32d1c6C4" || return 1
  register "SIVR" "0xc6100518997004eFb0701Da3c56000B3d093470a" || return 1
  register "SKHY" "0xa3a7BEcF428b250Ec1b88Dbc65F579a9570670c7" || return 1
  register "SMCI" "0x1eE2cE9654FFa008029e8e328a95f45EBfB3bC63" || return 1
  register "SPCX" "0x659Ea9dd1C833fc76E5328E0E616d8b9D9836dc3" || return 1
  register "SPYM" "0x484AaA9e6542774026b24aeD4EC3058400eD2439" || return 1
  register "TQQQ" "0xdf4F0897Ec6f0C37Bfc974a815cA440A3c2C2e8B" || return 1
  register "TR" "0xF88e511a3c762eE7E9ddd348a417F8e8db45BDEE" || return 1
  register "TSLA" "0xB41fD00d0bA60D9Ae8dCE405cB6AAd5710E5F84d" || return 1
  register "TSM" "0x7Ac659f601bEa2d9f490E0D0D8a68c4282fD9B21" || return 1
  register "TTWO" "0xb62E913f0cC881862527Fa7e41e1C98eEf09cedD" || return 1
  register "VWO" "0x2cc8DCfC649f9633C482C81473cC251226375fE3" || return 1
  register "WEN" "0x6c6f1CBe2fA860b1a15A02922d97A2d614db4923" || return 1
}
```

Run them one at a time, checking each response before the next. `POST` is
idempotent (a repeat of an identical registration is also a 201), but a 422
means the vault address already serves another underlying on Robinhood — stop
and reconcile rather than retrying.

```bash
issuer_curl() (
  local config_file

  config_file="$(mktemp)" || exit 1
  trap 'rm -f "$config_file"' EXIT
  chmod 600 "$config_file" || exit 1
  printf 'header = "X-API-KEY: %s"\n' "$ISSUER_API_KEY" \
    >"$config_file" || exit 1

  curl --config "$config_file" "$@"
)

register() {
  local underlying="$1" vault="$2"
  local response_file status

  response_file="$(mktemp)" || return

  status="$(issuer_curl -sS -o "$response_file" -w '%{http_code}' \
    "$ISSUER_BASE_URL/tokenized-assets/$underlying?network=robinhood")" || {
      rm -f "$response_file"
      return 1
    }

  case "$status" in
    200)
      if jq -e \
        --arg token "t$underlying" \
        --arg network "robinhood" \
        --arg vault "${vault,,}" \
        '.token == $token and .network == $network and
          (.vault | ascii_downcase) == $vault' \
        "$response_file" >/dev/null; then
        rm -f "$response_file"
        printf '%s is already registered with the expected values\n' \
          "$underlying"
        return 0
      fi

      printf '%s is already registered with different values; stopping\n' \
        "$underlying" >&2
      jq '{underlying, token, network, vault, status}' "$response_file" >&2
      rm -f "$response_file"
      return 1
      ;;
    404)
      ;;
    *)
      printf 'pre-check for %s returned HTTP %s; stopping\n' \
        "$underlying" "$status" >&2
      cat "$response_file" >&2
      rm -f "$response_file"
      return 1
      ;;
  esac

  status="$(issuer_curl -sS -o "$response_file" -w '%{http_code}' \
    -X POST "$ISSUER_BASE_URL/tokenized-assets" \
    -H 'Content-Type: application/json' \
    -d @- <<JSON
{
  "underlying": "$underlying",
  "token": "t$underlying",
  "network": "robinhood",
  "vault": "$vault"
}
JSON
  )" || {
    rm -f "$response_file"
    return 1
  }

  cat "$response_file"
  printf '\n%s\n' "$status"
  rm -f "$response_file"

  if [ "$status" != 201 ]; then
    printf 'registration for %s failed with HTTP %s; stopping\n' \
      "$underlying" "$status" >&2
    return 1
  fi
}

register_all
```

Expect `201` from the POST and `404` from the pre-check on a first run.

## Verification

After the last registration:

```bash
issuer_curl -sS \
  "$ISSUER_BASE_URL/tokenized-assets" | jq '
    [.tokens[] | select(.networks | index("robinhood"))] | length'
```

Expect `56`. Then restart the service once and confirm it comes back up.
Registration makes the Robinhood chain configuration permanently mandatory: from
here on `validate_configured_asset_networks` aborts the start unless
`CHAIN_ROBINHOOD_*` is present, whether or not the `[wrapped_tokens.robinhood]`
table is still in the config file.

Record in RAI-2284: the response code per symbol, the `/tokenized-assets` count,
the post-registration restart, and the backup taken beforehand.
