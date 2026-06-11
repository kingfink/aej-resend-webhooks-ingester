-- Resend Webhook Ingester - BigQuery schema
--
-- Vendored from:
--   https://github.com/resend/resend-webhooks-ingester
--   tag: v1.1.0
--   commit: c37a5e91ed6a5f4384cfccd5f19c9abbf64ac8ca
--
-- Local changes:
--   * Partition webhook tables by event_created_at instead of
--     webhook_received_at so event-time queries prune partitions.
--   * Preserve upstream clustering by event_type and entity identifier.
--   * Add resend_emails_backfill for latest-status historical snapshots.
--
-- Replace YOUR_PROJECT and YOUR_DATASET before running this file. For example:
--   sed -e 's/YOUR_PROJECT/my-project/g' \
--       -e 's/YOUR_DATASET/resend_webhooks/g' schemas/bigquery.sql | \
--     bq query --use_legacy_sql=false

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.resend_wh_emails` (
  id STRING NOT NULL,
  svix_id STRING NOT NULL,
  event_type STRING NOT NULL,
  webhook_received_at TIMESTAMP NOT NULL,
  event_created_at TIMESTAMP NOT NULL,
  email_id STRING NOT NULL,
  from_address STRING NOT NULL,
  to_addresses ARRAY<STRING>,
  subject STRING NOT NULL,
  email_created_at TIMESTAMP NOT NULL,
  broadcast_id STRING,
  template_id STRING,
  tags STRING,
  bounce_type STRING,
  bounce_sub_type STRING,
  bounce_message STRING,
  bounce_diagnostic_code ARRAY<STRING>,
  click_ip_address STRING,
  click_link STRING,
  click_timestamp TIMESTAMP,
  click_user_agent STRING
)
PARTITION BY DATE(event_created_at)
CLUSTER BY event_type, email_id;

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.resend_wh_contacts` (
  id STRING NOT NULL,
  svix_id STRING NOT NULL,
  event_type STRING NOT NULL,
  webhook_received_at TIMESTAMP NOT NULL,
  event_created_at TIMESTAMP NOT NULL,
  contact_id STRING NOT NULL,
  audience_id STRING,
  segment_ids ARRAY<STRING>,
  email STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  unsubscribed BOOL NOT NULL,
  contact_created_at TIMESTAMP NOT NULL,
  contact_updated_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(event_created_at)
CLUSTER BY event_type, contact_id;

CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.resend_wh_domains` (
  id STRING NOT NULL,
  svix_id STRING NOT NULL,
  event_type STRING NOT NULL,
  webhook_received_at TIMESTAMP NOT NULL,
  event_created_at TIMESTAMP NOT NULL,
  domain_id STRING NOT NULL,
  name STRING NOT NULL,
  status STRING NOT NULL,
  region STRING NOT NULL,
  domain_created_at TIMESTAMP NOT NULL,
  records STRING
)
PARTITION BY DATE(event_created_at)
CLUSTER BY event_type, domain_id;

-- One row per sent email. This is a latest-status snapshot, not event history.
CREATE TABLE IF NOT EXISTS `YOUR_PROJECT.YOUR_DATASET.resend_emails_backfill` (
  email_id STRING NOT NULL,
  to_addresses ARRAY<STRING>,
  from_address STRING,
  subject STRING,
  created_at TIMESTAMP NOT NULL,
  last_event STRING,
  backfilled_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(created_at)
CLUSTER BY last_event, email_id;
