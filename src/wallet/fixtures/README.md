# Turnkey fixtures

`turnkey_sign_raw_payload_result.json` contains the signature result from a real
Turnkey `ACTIVITY_TYPE_SIGN_RAW_PAYLOAD_V2` response captured on 2026-07-14 by
the ignored `turnkey_integration` test. The request signed a local-Anvil
transaction through the production Turnkey API.

Only the non-secret `r`, `s`, and `v` result fields are retained. The API key,
organization ID, wallet address, request stamp, activity ID, and other response
metadata are intentionally omitted. The response contract is documented at
<https://docs.turnkey.com/api-reference/activities/sign-raw-payload>.
