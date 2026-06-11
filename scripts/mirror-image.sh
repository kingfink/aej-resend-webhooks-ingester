#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and edit it first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

REGION="${REGION:-us-east4}"
AR_LOCATION="${AR_LOCATION:-$REGION}"
AR_REPO="${AR_REPO:-webhooks}"

: "${PROJECT_ID:?PROJECT_ID must be set in .env}"
: "${IMAGE_TAG:?IMAGE_TAG must be set in .env}"

if [[ "$PROJECT_ID" == "your-gcp-project-id" ]]; then
  echo "Replace the example PROJECT_ID in .env before mirroring the image." >&2
  exit 1
fi

if [[ "$IMAGE_TAG" == "latest" ]]; then
  echo "IMAGE_TAG must be a pinned upstream release, not latest." >&2
  exit 1
fi

for command_name in gcloud docker; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: $command_name" >&2
    exit 1
  fi
done

SOURCE_IMAGE="ghcr.io/resend/resend-webhooks-ingester:${IMAGE_TAG}"
TARGET_IMAGE="${AR_LOCATION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/resend-webhooks-ingester:${IMAGE_TAG}"

echo "Configuring Docker authentication for Artifact Registry..."
gcloud auth configure-docker "${AR_LOCATION}-docker.pkg.dev" --quiet

echo "Pulling $SOURCE_IMAGE..."
docker pull --platform=linux/amd64 "$SOURCE_IMAGE"

echo "Tagging $TARGET_IMAGE..."
docker tag "$SOURCE_IMAGE" "$TARGET_IMAGE"

echo "Pushing $TARGET_IMAGE..."
docker push "$TARGET_IMAGE"

echo "Mirrored image: $TARGET_IMAGE"
