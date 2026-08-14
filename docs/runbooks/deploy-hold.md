# Deployment hold

Use the deployment hold when maintenance requires the issuance service to stay
stopped across a deploy. While the hold exists, deployment installs and
validates the new profile but does not create the readiness marker or restart
the service.

## Arm the hold

Create the runtime directory and arm the hold before stopping the service. This
ensures a concurrent deploy observes the hold and leaves the service stopped:

```sh
install -d /run/st0x
touch /run/st0x/st0x-issuance.hold
rm -f /run/st0x/st0x-issuance.ready
systemctl stop st0x-issuance
```

Confirm the hold exists and the service is inactive before dispatching a deploy:

```sh
test -f /run/st0x/st0x-issuance.hold
test ! -e /run/st0x/st0x-issuance.ready
! systemctl is-active --quiet st0x-issuance
```

The repository's service-start, service-restart, and database-reset apps refuse
to run while the hold exists. Do not bypass that guard with direct `systemctl`
commands or by recreating the readiness marker.

The hold lives under `/run`, which is a temporary filesystem. A reboot clears
the hold, readiness marker, and decrypted service environment. If the
maintenance window is still open after a reboot, re-create the runtime
directory, arm the hold, and stop the service in the order above before any
deploy. Run a deploy to restore the decrypted environment before releasing the
hold and starting the service.

## Release the hold

Release the hold only after the maintenance procedure and post-operation checks
are complete. Remove the hold on the target host:

```sh
rm /run/st0x/st0x-issuance.hold
```

Then, from the deployment checkout, run the service deployment again so profile
activation validates the configuration, creates the readiness marker, and starts
the unit:

```sh
nix run .#prodDeployService -- -i "$SSH_IDENTITY" st0x-issuance
# Staging: nix run .#stagingDeployService -- -i "$SSH_IDENTITY" st0x-issuance
```

Do not create the readiness marker or start the unit manually. The deployment
activation owns both actions. Verify the marker exists, the unit is active, and
startup reconciliation completed without errors before resuming dependent
services:

```sh
test -f /run/st0x/st0x-issuance.ready
systemctl is-active --quiet st0x-issuance
journalctl -u st0x-issuance --since "10 minutes ago" --no-pager
```

## If validation or activation fails

If the held deployment fails validation, leave the hold armed and the service
inactive. `deploy-rs` exits non-zero and rolls back the failing profile; retain
the deployment output because `validate-config` errors occur before the unit
starts.

If the release deployment fails or the unit remains inactive, re-arm the hold
before another deployment can start the service, then inspect the unit status
and logs:

```sh
install -d /run/st0x
touch /run/st0x/st0x-issuance.hold
rm -f /run/st0x/st0x-issuance.ready
systemctl stop st0x-issuance
systemctl status st0x-issuance --no-pager
journalctl -u st0x-issuance --since "30 minutes ago" --no-pager
```

Do not bypass validation by creating the readiness marker. Escalate to the
deployment owner or incident lead; redeploy a last-known-good revision only
after they confirm that the maintenance operation and database remain compatible
with it.
