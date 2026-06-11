# Resend Webhooks on Cloud Run

This repository deploys Resend's upstream webhook ingester to Cloud Run, where
it verifies Svix signatures and streams email, contact, and domain events into
BigQuery. The upstream image runs unmodified; this repository contains only
configuration, deployment tooling, schema DDL, and a one-time backfill script.

## Quick Start

You need a GCP project with billing enabled, the Google Cloud CLI (`gcloud` and
`bq`), and Docker.

1. Authenticate and configure the project:

```bash
gcloud auth login
cp .env.example .env
$EDITOR .env
```

`REGION` controls Cloud Run. `AR_LOCATION` and `BQ_LOCATION` independently
control Artifact Registry and BigQuery; they may be regional values such as
`us-east1` or supported multi-regions such as `us` and `US`.

2. Provision GCP resources and create the BigQuery tables:

```bash
./scripts/setup.sh
```

3. Mirror the pinned upstream image and deploy it:

```bash
./scripts/mirror-image.sh
./deploy.sh
```

4. Create a webhook in the [Resend dashboard](https://resend.com/webhooks)
   pointing to the `/bigquery` URL printed by `deploy.sh`.

5. Install the webhook's real signing secret and redeploy:

```bash
./scripts/setup.sh --set-webhook-secret
./deploy.sh
```

Send a test event from Resend. That is the complete deployment path.

## Architecture

```text
Resend -> Cloud Run POST /bigquery -> BigQuery resend_webhooks dataset
                    |
                    +-> Svix signature verification
```

The Cloud Run service uses a dedicated service account and Application Default
Credentials. No service-account key or `BIGQUERY_CREDENTIALS` value is stored
or passed to the container.

The upstream schema is pinned to Git tag
[`v1.1.0`](https://github.com/resend/resend-webhooks-ingester/releases/tag/v1.1.0),
commit `c37a5e91ed6a5f4384cfccd5f19c9abbf64ac8ca`. GHCR publishes the corresponding
container as image tag `1.1.0`.

`setup.sh` enables the required APIs, creates the Artifact Registry repository,
service account, BigQuery dataset and tables, and Secret Manager secret. Its
first run generates a temporary bootstrap signing secret so Cloud Run can
start before the Resend webhook exists. Replace that value in step 5 before
sending real traffic.

The service account receives BigQuery Data Editor only on the configured
dataset, BigQuery Job User on the project, and access to the signing secret.
The mirror script selects `linux/amd64`, the architecture required by Cloud
Run, including when run from an Apple Silicon laptop.

## Upgrading the Image

1. Review the upstream
   [releases](https://github.com/resend/resend-webhooks-ingester/releases),
   changelog, Dockerfile diff, schema diff, and BigQuery connector changes.
2. Change `IMAGE_TAG` in `.env` to a specific release tag.
3. If the schema changed, update `schemas/bigquery.sql` and its provenance
   comment.
4. Mirror and deploy:

```bash
./scripts/mirror-image.sh
./deploy.sh
```

Never use an unpinned image tag.

## Backfill

The backfill writes latest-state snapshots to `resend_emails_backfill` and
`resend_contacts_backfill`; it never creates synthetic rows in the webhook
event tables. The sent-email API exposes one latest status per email, not the
historical sequence of delivered, opened, clicked, or other webhook events.

Contact snapshots include custom properties, segment memberships, and topic
subscriptions. Contacts missing from a later API scan remain in BigQuery with
their previous `last_seen_at`; the backfill does not infer that they were
deleted.

Create a local environment and authenticate Application Default Credentials:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
gcloud auth application-default login
source .env
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

Google client libraries use `GOOGLE_APPLICATION_CREDENTIALS` before the user
credentials created by `gcloud auth application-default login`. If that
environment variable points to a different service account, either grant that
account BigQuery access or unset it for the backfill process:

```bash
env -u GOOGLE_APPLICATION_CREDENTIALS python scripts/backfill.py
```

The active principal needs permission to create BigQuery jobs in the project
and edit data in the configured dataset. The script checks job creation before
requesting any pages from Resend and reports the active credential source when
that check fails.

Set `RESEND_API_KEY` in the ignored `.env` file, then run:

```bash
python scripts/backfill.py
```

The default command backfills emails and contacts. To run only one pipeline:

```bash
python scripts/backfill.py --emails-only
python scripts/backfill.py --contacts-only
```

To bound the email walk to messages created on or after a date:

```bash
python scripts/backfill.py --since 2026-01-01
```

`--since` does not limit contacts because the contact pipeline must scan the
complete list for `last_seen_at` to be meaningful. The script requests at most
100 objects per page, stays below approximately two requests per second,
retries rate limits and transient server errors, and merges on the Resend
object ID. Contact runs take longer because each contact requires additional
requests for properties, segments, and topics.

The webhook go-live date is the event-history floor for downstream models.
Record that date in your own operational documentation. Before that date, the
backfill provides only each email's latest known status.

## Verifying

Send a test event from the Resend dashboard, then count today's email events:

```sql
SELECT
  event_type,
  COUNT(*) AS event_count
FROM `YOUR_PROJECT.YOUR_DATASET.resend_wh_emails`
WHERE DATE(event_created_at) = CURRENT_DATE()
GROUP BY event_type
ORDER BY event_count DESC;
```

Read recent Cloud Run logs:

```bash
source .env
gcloud run services logs read "$SERVICE_NAME" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --limit=100
```

- HTTP `401` usually means the webhook signing secret does not match.
- HTTP `500` usually means the BigQuery schema, IAM, location, or query failed.
- Cloud Run logs are also available in Cloud Logging under the service name.

Cloud Run normally injects `PORT=8080`, which overrides the image's default and
is respected by the Next.js standalone server. If startup logs show the
container listening only on port 3000, add `--port=3000` to the
`gcloud run deploy` command in `deploy.sh` and redeploy.

## Gotchas

- A signing secret belongs to one webhook. Recreating the webhook rotates the
  secret; add the new version and redeploy.
- `--allow-unauthenticated` is intentional. Svix signature verification is the
  request-level access control, and invalid signatures receive `401`.
- BigQuery partition expiration is the retention lever if indefinite storage
  is no longer desired.
- Pre-webhook open and click history cannot be reconstructed from the List Sent
  Emails API. Only the latest status per email is available.
- The Cloud Run service account has BigQuery Data Editor only on this dataset.
  BigQuery Job User remains project-scoped because query jobs are project
  resources.

## TODO: Replay Experiment

After deployment, test whether Resend's Svix-backed webhook replay can deliver
events from before the endpoint existed, potentially within Svix's roughly
90-day retention window. If it works, replay provides true recent event-level
history; upstream storage is idempotent on Svix message ID, so duplicates are
safe. Record the tested date range and result here.
