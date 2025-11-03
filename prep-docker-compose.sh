#!/usr/bin/env bash
set -euo pipefail

PROD_MODE=false
SKIP_BUILD=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --prod)
      PROD_MODE=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    *)
      echo "Unknown argument: $1"
      echo "Usage: prep-docker-compose.sh [--prod] [--skip-build]"
      exit 1
      ;;
  esac
done

if [ "$PROD_MODE" = true ]; then
  echo "==> Production mode: using registry images"

  if [ -z "${REGISTRY_NAME:-}" ]; then
    echo "ERROR: REGISTRY_NAME environment variable is required for --prod mode"
    exit 1
  fi
  if [ -z "${SHORT_SHA:-}" ]; then
    echo "ERROR: SHORT_SHA environment variable is required for --prod mode"
    exit 1
  fi
  if [ -z "${DATA_VOLUME_PATH:-}" ]; then
    echo "ERROR: DATA_VOLUME_PATH environment variable is required for --prod mode"
    exit 1
  fi

  export DOCKER_IMAGE="registry.digitalocean.com/${REGISTRY_NAME}/issuance-bot:${SHORT_SHA}"
  export PULL_POLICY="always"
else
  echo "==> Local/debug mode: building image locally"

  export DOCKER_IMAGE="issuance-bot:local"
  export DATA_VOLUME_PATH="./data"
  export PULL_POLICY="never"

  mkdir -p "${DATA_VOLUME_PATH}"

  if [ "$SKIP_BUILD" = false ]; then
    if ! command -v docker &> /dev/null; then
      echo "ERROR: docker command not found. Please install Docker."
      exit 1
    fi
    echo "==> Building Docker image with debug profile..."
    docker build --build-arg BUILD_PROFILE=debug -t "${DOCKER_IMAGE}" .
  else
    echo "==> Skipping Docker image build (--skip-build)"
  fi
fi

echo "==> Generating docker-compose.yaml"
# shellcheck disable=SC2016
envsubst '$DOCKER_IMAGE $DATA_VOLUME_PATH $PULL_POLICY' < docker-compose.template.yaml > docker-compose.yaml

echo "==> docker-compose.yaml generated successfully"
echo "    DOCKER_IMAGE=$DOCKER_IMAGE"
echo "    DATA_VOLUME_PATH=$DATA_VOLUME_PATH"
echo "    PULL_POLICY=$PULL_POLICY"
