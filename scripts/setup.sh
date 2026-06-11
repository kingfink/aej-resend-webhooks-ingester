#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$ROOT_DIR/.env"
SCHEMA_FILE="$ROOT_DIR/schemas/bigquery.sql"
SECRET_NAME="resend-webhook-secret"
SET_WEBHOOK_SECRET=false

usage() {
  cat <<'EOF'
Usage: ./scripts/setup.sh [--set-webhook-secret]

Provision the GCP resources used by the Resend webhook ingester.

Options:
  --set-webhook-secret  Prompt for and add a new signing-secret version.
  -h, --help            Show this help.
EOF
}

while (($# > 0)); do
  case "$1" in
    --set-webhook-secret)
      SET_WEBHOOK_SECRET=true
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy .env.example to .env and edit it first." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

REGION="${REGION:-us-east4}"
AR_REPO="${AR_REPO:-webhooks}"
BQ_DATASET_ID="${BQ_DATASET_ID:-resend_webhooks}"
SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-resend-ingester}"

: "${PROJECT_ID:?PROJECT_ID must be set in .env}"

if [[ "$PROJECT_ID" == "your-gcp-project-id" ]]; then
  echo "Replace the example PROJECT_ID in .env before running setup." >&2
  exit 1
fi

for command_name in gcloud bq openssl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command not found: $command_name" >&2
    exit 1
  fi
done

SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Enabling required APIs..."
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  bigquery.googleapis.com \
  --project="$PROJECT_ID"

if ! gcloud artifacts repositories describe "$AR_REPO" \
  --location="$REGION" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Creating Artifact Registry repository $AR_REPO..."
  gcloud artifacts repositories create "$AR_REPO" \
    --repository-format=docker \
    --location="$REGION" \
    --description="Mirrored images for webhook services" \
    --project="$PROJECT_ID"
else
  echo "Artifact Registry repository $AR_REPO already exists."
fi

if ! gcloud iam service-accounts describe "$SERVICE_ACCOUNT_EMAIL" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Creating service account $SERVICE_ACCOUNT_NAME..."
  gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
    --display-name="Resend webhook ingester" \
    --project="$PROJECT_ID"
else
  echo "Service account $SERVICE_ACCOUNT_NAME already exists."
fi

if ! bq --project_id="$PROJECT_ID" show \
  "${PROJECT_ID}:${BQ_DATASET_ID}" >/dev/null 2>&1; then
  echo "Creating BigQuery dataset $BQ_DATASET_ID in $REGION..."
  bq --project_id="$PROJECT_ID" \
    --location="$REGION" \
    mk --dataset "${PROJECT_ID}:${BQ_DATASET_ID}"
else
  echo "BigQuery dataset $BQ_DATASET_ID already exists."
fi

echo "Applying BigQuery schema..."
sed \
  -e "s/YOUR_PROJECT/${PROJECT_ID}/g" \
  -e "s/YOUR_DATASET/${BQ_DATASET_ID}/g" \
  "$SCHEMA_FILE" |
  bq --project_id="$PROJECT_ID" \
    --location="$REGION" \
    query --use_legacy_sql=false

echo "Granting dataset-scoped BigQuery Data Editor..."
bq --project_id="$PROJECT_ID" \
  --location="$REGION" \
  query --use_legacy_sql=false \
  "GRANT \`roles/bigquery.dataEditor\` ON SCHEMA \`${PROJECT_ID}\`.${BQ_DATASET_ID} TO \"serviceAccount:${SERVICE_ACCOUNT_EMAIL}\""

echo "Granting project-level BigQuery Job User..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/bigquery.jobUser" \
  --condition=None \
  --quiet

if ! gcloud secrets describe "$SECRET_NAME" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Creating Secret Manager secret $SECRET_NAME..."
  gcloud secrets create "$SECRET_NAME" \
    --replication-policy=automatic \
    --project="$PROJECT_ID"
else
  echo "Secret Manager secret $SECRET_NAME already exists."
fi

echo "Granting the service account access to $SECRET_NAME..."
gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --project="$PROJECT_ID" \
  --quiet

add_secret_version() {
  local secret_value="$1"
  if [[ "$secret_value" != whsec_* ]]; then
    echo "The signing secret must begin with whsec_." >&2
    exit 1
  fi

  printf '%s' "$secret_value" | gcloud secrets versions add "$SECRET_NAME" \
    --data-file=- \
    --project="$PROJECT_ID"
}

if [[ "$SET_WEBHOOK_SECRET" == "true" ]]; then
  if [[ ! -t 0 ]]; then
    echo "A terminal is required to enter the signing secret securely." >&2
    exit 1
  fi

  read -r -s -p "Paste the Resend webhook signing secret (input hidden): " secret_value
  echo
  if [[ -z "$secret_value" ]]; then
    echo "The signing secret cannot be empty." >&2
    exit 1
  fi
  add_secret_version "$secret_value"
elif [[ -z "$(gcloud secrets versions list "$SECRET_NAME" \
  --filter='state=ENABLED' \
  --limit=1 \
  --format='value(name)' \
  --project="$PROJECT_ID")" ]]; then
  bootstrap_secret="whsec_$(openssl rand -base64 32 | tr -d '\n')"
  add_secret_version "$bootstrap_secret"
  echo "Added a temporary bootstrap signing-secret version."
else
  echo "An enabled signing-secret version already exists."
  echo "Run ./scripts/setup.sh --set-webhook-secret to add a new version."
fi

cat <<EOF

Setup complete.
Project:         $PROJECT_ID
Region:          $REGION
Dataset:         $BQ_DATASET_ID
Service account: $SERVICE_ACCOUNT_EMAIL
Secret:          $SECRET_NAME
EOF
