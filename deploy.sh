#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$ROOT_DIR/.env"
SECRET_NAME="resend-webhook-secret"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and edit it first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

REGION="${REGION:-us-east4}"
AR_REPO="${AR_REPO:-webhooks}"
SERVICE_NAME="${SERVICE_NAME:-resend-webhooks-ingester}"
BQ_DATASET_ID="${BQ_DATASET_ID:-resend_webhooks}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-resend-ingester}"

: "${PROJECT_ID:?PROJECT_ID must be set in .env}"
: "${IMAGE_TAG:?IMAGE_TAG must be set in .env}"

if [[ "$PROJECT_ID" == "your-gcp-project-id" ]]; then
  echo "Replace the example PROJECT_ID in .env before deploying." >&2
  exit 1
fi

if [[ "$IMAGE_TAG" == "latest" ]]; then
  echo "IMAGE_TAG must be a pinned upstream release, not latest." >&2
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "Required command not found: gcloud" >&2
  exit 1
fi

IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/resend-webhooks-ingester:${IMAGE_TAG}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run deploy "$SERVICE_NAME" \
  --image="$IMAGE" \
  --service-account="$SERVICE_ACCOUNT_EMAIL" \
  --set-env-vars="BIGQUERY_PROJECT_ID=${PROJECT_ID},BIGQUERY_DATASET_ID=${BQ_DATASET_ID}" \
  --set-secrets="RESEND_WEBHOOK_SECRET=${SECRET_NAME}:latest" \
  --allow-unauthenticated \
  --region="$REGION" \
  --min-instances=0 \
  --project="$PROJECT_ID" \
  --quiet

SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  --format='value(status.url)')"

cat <<EOF

Deployment complete.
Service URL:     $SERVICE_URL
Webhook endpoint: ${SERVICE_URL}/bigquery
EOF
