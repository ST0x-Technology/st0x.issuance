# Fireblocks decommission

The code and deployment integration are retired, but workspace and encrypted
environment cleanup remain operator-owned follow-ups. Do not restore the deleted
migration CLI or key files to perform them.

## On-chain retirement evidence

At Base block `49,821,087`, public read-only RPC calls confirmed the retired
issuer wallet `0x1c66D6708914C40239D54919320b4C48cAE3D1A9` held zero of every
legacy item the deleted sweep command knew about:

| Asset                 | Contract                                     | Identifier | Source balance |
| --------------------- | -------------------------------------------- | ---------: | -------------: |
| tCOIN receipt         | `0xBA1B8836A5510815e96103F067715b7CCC7c2E0E` |         19 |              0 |
| tCRCL receipt         | `0xd508B97975fBE04E62bFf18959549b046bD8FA78` |          4 |              0 |
| tMSTR receipt         | `0x1c1fEF6f7b8e576219554b1d11c8aF29D00C0cEC` |          5 |              0 |
| tSPYM receipt         | `0x957056dD6e2E594742E36675e8AA5A567163E5bd` |         10 |              0 |
| tSPYM receipt         | `0x957056dD6e2E594742E36675e8AA5A567163E5bd` |         12 |              0 |
| stranded tCOIN shares | `0x626757e6F50675D17fcAd312E82f989aE7A23d38` |        n/a |              0 |

These readings prove there is nothing left at the retired source for the deleted
tool to sweep. They do not replace historical transaction review when an
operator needs to prove where a particular balance went.

## Operator follow-ups

Complete these outside this PR after confirming the production and staging
deployments no longer load Fireblocks configuration:

1. Remove `FIREBLOCKS_*` entries from both encrypted service environments using
   the repository editor; never decrypt them to a persistent plaintext file:

   ```sh
   nix run .#secret -- secret/st0x-issuance-prod.env.age -i "$SSH_IDENTITY"
   nix run .#secret -- secret/st0x-issuance-staging.env.age -i "$SSH_IDENTITY"
   ```

2. Deploy each environment and confirm the generated service environment has no
   `FIREBLOCKS_*` names. The activation also deletes the retired plaintext key
   path `/run/agenix/fireblocks-secret-issuance.key` idempotently.
3. Review the retired wallet's complete asset inventory and transaction history
   for anything outside the explicit zero-balance table above. Escalate any
   unexplained balance or destination before closing the workspace account, and
   sweep anything found while the API user and policy rule still exist.
4. In Fireblocks, revoke the issuer API user and its credentials only after the
   inventory review is complete and no remaining automation depends on them.
5. Remove the temporary Transaction Authorization Policy rule and its operator
   group membership. Then remove each Receipt contract's Base asset entry and
   delete the contract-wallet whitelist container if it has no other use.

Policy cleanup must follow asset verification: deleting authorization first can
strand an item that was missed by the known legacy table.
