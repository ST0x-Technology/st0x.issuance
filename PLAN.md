# Continuous Deployment Setup Implementation Plan

This plan implements continuous deployment (CD) for the issuance bot based on
the proven setup from `st0x.liquidity-a`.

**Reference implementation:** `/Users/0xgleb/code/st0x/st0x.liquidity-a/`

**Development approach:** Iterative development using GitHub Actions for
testing. Each task produces a working workflow that can be tested in CI before
moving to the next task.

## Overview

The CD setup will enable:

- Automatic deployment to production on merges to `main` branch
- Manual deployment via workflow dispatch
- Docker containerization with multi-stage builds using Nix
- Deployment to DigitalOcean Droplet via SSH
- Container registry hosting on DigitalOcean
- Automatic rollback on deployment failures
- Manual rollback workflow
- Comprehensive deployment logging and health checks

**Design Decisions:**

1. **Single-service architecture**: Unlike liquidity-a which runs multiple
   broker services (schwarbot, alpacabot, reporter-schwab, reporter-alpaca),
   issuance-a runs a single HTTP server binary. This simplifies the
   docker-compose setup.

2. **No Grafana in initial setup**: liquidity-a includes Grafana + OTLP metrics.
   For the initial CD setup, we'll focus on core deployment functionality.
   Grafana integration can be added later when implementing issue #31 (Integrate
   Grafana for metrics).

3. **Iterative testing via GitHub Actions**: Build and test incrementally in GH
   Actions to avoid long local build times. Start with dummy/minimal
   implementations and gradually add functionality.

---

## Task 1. Create minimal Dockerfile and basic build workflow

Create a simple Dockerfile and GitHub Actions workflow to establish the build
pipeline. Test that the basic infrastructure works before adding complexity.

**Reference:** `/Users/0xgleb/code/st0x/st0x.liquidity-a/Dockerfile` (for final
structure)

**Subtasks:**

- [x] Create a minimal Dockerfile that builds successfully but doesn't do full
      Nix build yet
- [x] Create `.github/workflows/deploy.yaml` with just the build steps (no
      deployment yet)
- [x] Configure workflow to trigger on push to current branch (for testing)
- [x] Add Docker build step with proper tagging
- [x] Verify workflow runs successfully in GitHub Actions

**Minimal Dockerfile approach:**

Option 1 - Dummy Dockerfile for initial testing:

```dockerfile
FROM debian:12-slim
WORKDIR /app
RUN echo "#!/bin/sh\necho 'Hello from issuance-bot'\nsleep infinity" > server && chmod +x server
CMD ["./server"]
```

Option 2 - Simple Rust build without Nix (faster iteration):

```dockerfile
FROM rust:1.80 AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM debian:12-slim
COPY --from=builder /app/target/release/server ./
CMD ["./server"]
```

**Minimal workflow (no deployment, no registry push yet):**

```yaml
name: Build and deploy

on:
  push:
    branches: [feat/cd] # Test on feature branch first
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - name: Build Docker image
        run: |
          docker build -t issuance-bot:test .

      - name: Test image runs
        run: |
          docker run --rm -d --name test-container issuance-bot:test
          sleep 5
          docker logs test-container
          docker stop test-container
```

**Success criteria:**

- Workflow runs without errors
- Docker image builds successfully
- Container starts and runs

---

## Task 2. Add DigitalOcean registry push

Add Docker registry authentication and image push to verify the full
build-and-push pipeline works.

**Reference:**
`/Users/0xgleb/code/st0x/st0x.liquidity-a/.github/workflows/deploy.yaml` (lines
31-66)

**Subtasks:**

- [x] Add `DIGITALOCEAN_ACCESS_TOKEN` and `REGISTRY_NAME` to workflow env
- [x] Install doctl action
- [x] Add registry login step
- [x] Add image tagging with SHORT_SHA
- [x] Add registry push steps
- [x] Verify images appear in DigitalOcean Container Registry

**Workflow additions:**

```yaml
env:
  DIGITALOCEAN_ACCESS_TOKEN: ${{ secrets.DIGITALOCEAN_ACCESS_TOKEN }}
  REGISTRY_NAME: stox

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive

      - name: Install doctl
        uses: digitalocean/action-doctl@v2
        with:
          token: ${{ env.DIGITALOCEAN_ACCESS_TOKEN }}

      - name: Build Docker image
        run: |
          SHORT_SHA=$(echo ${{ github.sha }} | cut -c1-7)
          echo "SHORT_SHA=$SHORT_SHA" >> $GITHUB_ENV

          docker build \
            -t registry.digitalocean.com/${{ env.REGISTRY_NAME }}/issuance-bot:$SHORT_SHA \
            -t registry.digitalocean.com/${{ env.REGISTRY_NAME }}/issuance-bot:latest .

      - name: Log in to DO Container Registry
        run: doctl registry login --expiry-seconds 1200

      - name: Push image to DO Registry
        run: |
          docker push registry.digitalocean.com/${{ env.REGISTRY_NAME }}/issuance-bot:${{ env.SHORT_SHA }}
          docker push registry.digitalocean.com/${{ env.REGISTRY_NAME }}/issuance-bot:latest
```

**Success criteria:**

- Workflow authenticates with DigitalOcean
- Images are tagged correctly
- Images successfully push to registry
- Images visible in DO Container Registry UI

---

## Task 3. Add full Nix-based Dockerfile

Replace the minimal Dockerfile with the full Nix-based multi-stage build
including tests, migrations, and proper binary preparation.

**Reference:** `/Users/0xgleb/code/st0x/st0x.liquidity-a/Dockerfile` (all 85
lines)

**Subtasks:**

- [x] Replace Dockerfile with full Nix-based builder stage
- [x] Add cargo chef for dependency caching
- [x] Add SQLite database creation and migration steps
- [x] Add test execution during build
- [x] Add binary compilation with release profile
- [x] Fix binary interpreter paths with patchelf
- [x] Create minimal runtime stage
- [x] Test full build in GitHub Actions

**Key sections from reference to adapt:**

**Builder stage with Nix (lines 1-12):**

```dockerfile
FROM ubuntu:latest AS builder

ARG BUILD_PROFILE=release

RUN apt update -y
RUN apt install curl git -y
RUN curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install linux \
  --extra-conf "sandbox = false" \
  --extra-conf "experimental-features = nix-command flakes" \
  --init none \
  --no-confirm
ENV PATH="${PATH}:/nix/var/nix/profiles/default/bin"
```

**Cargo chef preparation (lines 14-37):**

```dockerfile
WORKDIR /app

COPY flake.nix flake.lock ./
RUN nix develop --command echo "Nix dev env ready"

COPY Cargo.toml Cargo.lock ./
# Note: issuance-a has no crates/ subdirectory, so skip that

RUN mkdir -p src && \
    echo 'fn main() {}' > src/lib.rs

RUN nix develop --command cargo chef prepare --recipe-path recipe.json

RUN rm -rf src

RUN nix develop --command bash -c ' \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo chef cook --release --recipe-path recipe.json; \
    else \
        cargo chef cook --recipe-path recipe.json; \
    fi'
```

**Build and test (lines 39-62, skip line 41 prepSolArtifacts):**

```dockerfile
COPY . .

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    sqlx database create && \
    sqlx migrate run \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    cargo test -q \
'

RUN nix develop --command bash -c ' \
    export DATABASE_URL=sqlite:///tmp/build_db.sqlite && \
    if [ "$BUILD_PROFILE" = "release" ]; then \
        cargo build --release --bin server; \
    else \
        cargo build --bin server; \
    fi'
```

**Patchelf (lines 65-68, only server binary):**

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends patchelf && \
    patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 /app/target/${BUILD_PROFILE}/server && \
    apt-get remove -y patchelf && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
```

**Runtime stage (lines 70-85, only server binary):**

```dockerfile
FROM debian:12-slim

ARG BUILD_PROFILE=release

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/${BUILD_PROFILE}/server ./
COPY --from=builder /app/migrations ./migrations
```

**Success criteria:**

- Full build completes in GitHub Actions
- Tests pass during build
- Binary is properly patched
- Image is pushed to registry
- Build time is reasonable (Nix caching works)

---

## Task 4. Create docker-compose template and preparation script

Create template-based docker-compose configuration and script to support both
local and production deployment.

**References:**

- `/Users/0xgleb/code/st0x/st0x.liquidity-a/docker-compose.template.yaml`
- `/Users/0xgleb/code/st0x/st0x.liquidity-a/prep-docker-compose.sh`

**Subtasks:**

- [x] Create `docker-compose.template.yaml` with single issuance-bot service
- [x] Create `prep-docker-compose.sh` script based on reference
- [x] Test locally with `./prep-docker-compose.sh` (uses local build or
      skip-build)
- [ ] Commit both files

**docker-compose.template.yaml:**

```yaml
services:
  issuance-bot:
    image: ${DOCKER_IMAGE}
    pull_policy: ${PULL_POLICY}
    container_name: issuance-bot
    command: ["./server"]
    environment:
      - DATABASE_URL=sqlite:///data/issuance.db
    env_file:
      - .env
    volumes:
      - ${DATA_VOLUME_PATH}:/data
    restart: unless-stopped
```

**prep-docker-compose.sh structure from reference:**

- Lines 1-24: Argument parsing (`--prod`, `--skip-build`)
- Lines 26-75: Mode-specific configuration (local vs production)
- Lines 77-87: Template generation with envsubst

**Key adaptations:**

- Remove Grafana-related variables
- Single service instead of multiple (schwarbot, alpacabot, reporters)
- Simpler environment variable substitution:
  `$DOCKER_IMAGE $DATA_VOLUME_PATH $PULL_POLICY`

**Success criteria:**

- Script runs locally without errors
- Generates valid docker-compose.yaml
- Can start container locally (if desired to test, but not required)

---

## Task 5. Update .env.example with deployment variables

Expand .env.example to include all required environment variables for production
deployment.

**Reference:** `/Users/0xgleb/code/st0x/st0x.liquidity-a/.env.example`

**Subtasks:**

- [ ] Add blockchain configuration (WS_RPC_URL, CHAIN_ID, vault addresses)
- [ ] Add Alpaca API credentials
- [ ] Add server configuration
- [ ] Add database URL
- [ ] Add logging configuration
- [ ] Add comments explaining each variable

**Template structure:**

```bash
# Blockchain RPC endpoint
# Format: wss://... or https://...
# Recommended: dRPC (https://drpc.org) - Free tier available
WS_RPC_URL=${WS_RPC_URL}

# Blockchain configuration
CHAIN_ID=${CHAIN_ID}
VAULT_ADDRESS=${VAULT_ADDRESS}
REDEMPTION_WALLET_ADDRESS=${REDEMPTION_WALLET_ADDRESS}

# Alpaca API
ALPACA_API_KEY=${ALPACA_API_KEY}
ALPACA_API_SECRET=${ALPACA_API_SECRET}
ALPACA_BASE_URL=${ALPACA_BASE_URL}

# Server
SERVER_HOST=${SERVER_HOST}
SERVER_PORT=${SERVER_PORT}
SERVER_API_KEY=${SERVER_API_KEY}

# Database
DATABASE_URL=${DATABASE_URL}

# Logging
RUST_LOG=${RUST_LOG}
```

**Success criteria:**

- All deployment variables documented
- Comments explain where to get values
- Format matches deployment workflow expectations

---

## Task 6. Add deployment steps to workflow

Add SSH deployment steps to the GitHub Actions workflow. Test deployment to the
droplet.

**Reference:**
`/Users/0xgleb/code/st0x/st0x.liquidity-a/.github/workflows/deploy.yaml` (lines
48-341)

**Subtasks:**

- [ ] Add file encoding step for .env.example, docker-compose.template.yaml,
      prep-docker-compose.sh
- [ ] Add SSH deployment action with appleboy/ssh-action
- [ ] Add deployment script with logging, backup, validation, and container
      checks
- [ ] Test deployment to droplet via workflow_dispatch
- [ ] Verify container starts and runs on droplet

**File encoding (lines 54-62 adapted):**

```yaml
- name: Encode file contents
  run: |
    echo "ENV_TEMPLATE=$(base64 -w 0 .env.example)" >> $GITHUB_ENV
    echo "DOCKER_COMPOSE_TEMPLATE=$(base64 -w 0 docker-compose.template.yaml)" >> $GITHUB_ENV
    echo "PREP_SCRIPT=$(base64 -w 0 prep-docker-compose.sh)" >> $GITHUB_ENV
```

**SSH deployment structure (lines 68-341 adapted):**

Key sections to include:

1. **Logging setup** (lines 78-83): Create `/var/log/issuance-bot/` directory
   and log files
2. **Helper functions** (lines 84-192): `log_exec`, `validate_required_env`,
   `check_container_running`, `check_container_logs`
3. **Configuration backup** (lines 212-224): Backup current config for rollback
   capability
4. **File creation** (lines 226-242): Decode and create files from base64
5. **Docker operations** (lines 244-271): Pull, down, up with error handling
6. **Container verification** (lines 282-308): Check containers running and logs
   for errors
7. **Cleanup** (lines 310-323): Remove old images and logs

**Simplifications for single-service:**

- Line 287: Only check `issuance-bot` container (not schwarbot, alpacabot,
  reporters)
- Lines 305-308: Only check `issuance-bot` logs
- Lines 300-303: Only capture `issuance-bot` logs

**Environment variables (lines 327-340 adapted):**

```yaml
env:
  WS_RPC_URL: ${{ secrets.WS_RPC_URL }}
  CHAIN_ID: ${{ secrets.CHAIN_ID }}
  VAULT_ADDRESS: ${{ secrets.VAULT_ADDRESS }}
  REDEMPTION_WALLET_ADDRESS: ${{ secrets.REDEMPTION_WALLET_ADDRESS }}
  ALPACA_API_KEY: ${{ secrets.ALPACA_API_KEY }}
  ALPACA_API_SECRET: ${{ secrets.ALPACA_API_SECRET }}
  ALPACA_BASE_URL: ${{ secrets.ALPACA_BASE_URL }}
  SERVER_API_KEY: ${{ secrets.SERVER_API_KEY }}
  ENV_TEMPLATE: ${{ env.ENV_TEMPLATE }}
  PREP_SCRIPT: ${{ env.PREP_SCRIPT }}
  DOCKER_COMPOSE_TEMPLATE: ${{ env.DOCKER_COMPOSE_TEMPLATE }}
```

**Success criteria:**

- Workflow successfully connects to droplet
- Files are created on droplet
- Container is pulled and started
- Health checks pass
- Logs show successful deployment

---

## Task 7. Add automatic rollback on deployment failure

Add rollback functionality to the deployment script so failed deployments
automatically restore the previous version.

**Reference:**
`/Users/0xgleb/code/st0x/st0x.liquidity-a/.github/workflows/deploy.yaml` (lines
92-144, 247-296)

**Subtasks:**

- [ ] Add `perform_rollback` function to deployment script
- [ ] Set up ERR trap to trigger rollback on failures
- [ ] Test rollback by intentionally failing deployment (e.g., bad image tag)
- [ ] Verify system restores to previous version
- [ ] Remove test failure and verify normal deployment works

**Rollback function (lines 92-144):**

```bash
perform_rollback() {
  trap - ERR  # Disable ERR trap to prevent re-entrancy
  echo "$(date '+%Y-%m-%d %H:%M:%S') - DEPLOYMENT FAILED - Attempting automatic rollback" | tee -a "${DEPLOY_LOG}" >&2

  cd /mnt/volume_nyc3_01

  # Check if required backup files exist
  if [ ! -f "docker-compose.yaml.backup" ] || [ ! -f ".env.backup" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - No backup files found, cannot rollback (possibly first deployment)" | tee -a "${DEPLOY_LOG}" >&2
    exit 1
  fi

  # Stop failed containers
  docker compose down 2>&1 | tee -a "${DEPLOY_LOG}" || true

  # Restore backed-up configuration
  cp -p docker-compose.yaml.backup docker-compose.yaml
  cp -p .env.backup .env

  # Restart with old configuration
  docker compose up -d 2>&1 | tee -a "${DEPLOY_LOG}"
  sleep 10

  # Verify rollback succeeded
  docker compose ps 2>&1 | tee -a "${DEPLOY_LOG}"

  echo "$(date '+%Y-%m-%d %H:%M:%S') - Rollback completed - production restored to previous version" | tee -a "${DEPLOY_LOG}" >&2
  exit 1
}
```

**ERR trap setup (lines 247-248):**

```bash
# Enable ERR trap for automatic rollback during risky window
trap 'perform_rollback' ERR
```

**Disable trap after success (lines 295-296):**

```bash
# Disable ERR trap - deployment successful
trap - ERR
```

**Success criteria:**

- Failed deployments automatically trigger rollback
- System returns to previous working state
- Normal deployments work without triggering rollback
- Logs clearly show rollback occurred

---

## Task 8. Create manual rollback workflow

Create a separate GitHub Actions workflow for manual rollback to previous
deployment.

**Reference:**
`/Users/0xgleb/code/st0x/st0x.liquidity-a/.github/workflows/rollback.yaml`

**Subtasks:**

- [ ] Create `rollback.sh` script based on reference
- [ ] Create `.github/workflows/rollback.yaml` based on reference
- [ ] Test manual rollback via workflow_dispatch
- [ ] Verify container is restored to previous version

**rollback.sh structure (213 lines from reference):**

- Lines 1-27: Header comments and setup
- Lines 28-61: Argument parsing (`--dry-run` flag)
- Lines 63-136: Dry-run validation checks
- Lines 139-213: Actual rollback execution

**Simplifications for issuance-a:**

- Remove Grafana backup restoration (lines 99-110, 187-200 in reference)
- Update paths to use `issuance-bot` naming

**Workflow structure (84 lines from reference):**

- Lines 1-27: Workflow metadata and confirmation input
- Lines 29-39: Checkout and script encoding
- Lines 36-84: SSH execution with logging and verification

**Only change needed (lines 76-77):**

```bash
docker compose logs --tail 50 issuance-bot 2>&1 | tee -a "${ROLLBACK_LOG}"
```

**Success criteria:**

- Rollback workflow requires "rollback" confirmation
- Script successfully restores previous deployment
- Containers start with restored configuration
- Logs capture rollback process

---

## Task 9. Switch workflow trigger to main branch

Update the deploy workflow to trigger on main branch instead of feature branch.

**Subtasks:**

- [ ] Update workflow trigger from `feat/cd` to `main`
- [ ] Add concurrency control to prevent parallel deployments
- [ ] Test by merging to main

**Workflow changes:**

```yaml
on:
  push:
    branches: [main]
  workflow_dispatch:

concurrency:
  group: ${{ github.ref }}-deploy
  cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}
```

**Success criteria:**

- Deployment only triggers on main branch pushes
- Manual deployments still work via workflow_dispatch
- Concurrent deployments are prevented

---

## Task 10. Update documentation

Update README.md with deployment information and instructions.

**Subtasks:**

- [ ] Add "Deployment" section to README.md
- [ ] Document local Docker development workflow
- [ ] Document production deployment process
- [ ] Document rollback process
- [ ] List required GitHub secrets
- [ ] Add troubleshooting section

**Documentation structure:**

````markdown
## Deployment

### Local Development with Docker

Build and run with Docker:

```bash
./prep-docker-compose.sh
docker compose up -d
```
````

View logs:

```bash
docker compose logs -f issuance-bot
```

### Production Deployment

Automatic deployment on push to `main`:

- Builds Docker image with Nix
- Runs tests during build
- Pushes to DigitalOcean Container Registry
- Deploys to production droplet via SSH
- Automatically rolls back on failure

Manual deployment:

1. Go to GitHub Actions
2. Select "Build and deploy" workflow
3. Click "Run workflow"

### Rollback

To rollback to previous deployment:

1. Go to GitHub Actions
2. Select "Rollback deployment" workflow
3. Click "Run workflow"
4. Type "rollback" to confirm

### Required GitHub Secrets

[List all secrets]

### Troubleshooting

[Common issues and solutions]

```
**Success criteria:**
- README includes complete deployment documentation
- All workflows and scripts are documented
- Troubleshooting section covers common issues

---

## Required GitHub Secrets

Configure these secrets in the GitHub repository settings:

**Infrastructure:**
- `DIGITALOCEAN_ACCESS_TOKEN` - DigitalOcean API token for registry access
- `DROPLET_HOST` - Production droplet hostname or IP address
- `DROPLET_SSH_KEY` - SSH private key for droplet access

**Blockchain:**
- `WS_RPC_URL` - WebSocket RPC endpoint for blockchain monitoring
- `CHAIN_ID` - Blockchain chain ID (e.g., 8453 for Base)
- `VAULT_ADDRESS` - Rain OffchainAssetReceiptVault contract address
- `REDEMPTION_WALLET_ADDRESS` - Address to monitor for redemption transfers

**Alpaca:**
- `ALPACA_API_KEY` - Alpaca API key
- `ALPACA_API_SECRET` - Alpaca API secret
- `ALPACA_BASE_URL` - Alpaca API base URL (e.g., https://api.alpaca.markets)

**Server:**
- `SERVER_API_KEY` - API key for authenticating requests to our endpoints
- `SERVER_HOST` - Server host (e.g., 0.0.0.0)
- `SERVER_PORT` - Server port (e.g., 8000)
```
