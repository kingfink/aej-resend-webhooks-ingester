#!/usr/bin/env python3
"""Backfill Resend email and contact snapshots into BigQuery."""

from __future__ import annotations

import argparse
import os
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery

API_BASE_URL = "https://api.resend.com"
EMAIL_BACKFILL_TABLE = "resend_emails_backfill"
CONTACT_BACKFILL_TABLE = "resend_contacts_backfill"
USER_AGENT = "resend-webhooks-cloudrun-backfill/1.0"
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 30

EMAIL_BACKFILL_SCHEMA = [
    bigquery.SchemaField("email_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("to_addresses", "STRING", mode="REPEATED"),
    bigquery.SchemaField("from_address", "STRING"),
    bigquery.SchemaField("subject", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("last_event", "STRING"),
    bigquery.SchemaField("backfilled_at", "TIMESTAMP", mode="REQUIRED"),
]

CONTACT_BACKFILL_SCHEMA = [
    bigquery.SchemaField("contact_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("unsubscribed", "BOOLEAN", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("properties", "JSON"),
    bigquery.SchemaField("segment_ids", "STRING", mode="REPEATED"),
    bigquery.SchemaField(
        "topics",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("topic_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("subscription", "STRING"),
        ],
    ),
    bigquery.SchemaField("backfilled_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("last_seen_at", "TIMESTAMP", mode="REQUIRED"),
]


def load_env_file(path: Path) -> None:
    """Load the repository's simple KEY=VALUE .env format without overriding env."""
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected DATE in YYYY-MM-DD format") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Resend email and contact snapshots into BigQuery."
    )
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--emails-only",
        action="store_true",
        help="Backfill email snapshots only.",
    )
    selection.add_argument(
        "--contacts-only",
        action="store_true",
        help="Backfill contact snapshots only.",
    )
    parser.add_argument(
        "--since",
        type=parse_date,
        help=(
            "Include emails created on or after this date (YYYY-MM-DD). "
            "Does not limit contacts."
        ),
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("PROJECT_ID"),
        help="GCP project ID. Defaults to PROJECT_ID from the environment.",
    )
    parser.add_argument(
        "--dataset-id",
        default=os.environ.get("BQ_DATASET_ID", "resend_webhooks"),
        help="BigQuery dataset ID. Defaults to BQ_DATASET_ID.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        choices=range(1, 101),
        metavar="1-100",
        help="Resend API page size (default: 100).",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.55,
        help="Seconds between Resend requests (default: 0.55, under 2 req/s).",
    )
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class ResendClient:
    def __init__(
        self,
        session: requests.Session,
        api_key: str,
        *,
        request_delay: float,
    ) -> None:
        self.session = session
        self.request_delay = max(request_delay, 0.0)
        self.last_request_at: float | None = None
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }

    def _wait_for_request_slot(self) -> None:
        if self.last_request_at is None:
            return
        remaining = self.request_delay - (time.monotonic() - self.last_request_at)
        if remaining > 0:
            time.sleep(remaining)

    def get(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
    ) -> dict[str, Any]:
        for attempt in range(MAX_RETRIES + 1):
            self._wait_for_request_slot()
            response = self.session.get(
                f"{API_BASE_URL}{path}",
                headers=self.headers,
                params=params or None,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            self.last_request_at = time.monotonic()

            if response.status_code != 429 and response.status_code < 500:
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ValueError("Resend response was not a JSON object")
                return payload

            if attempt == MAX_RETRIES:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            try:
                delay = float(retry_after) if retry_after else 2**attempt
            except ValueError:
                delay = 2**attempt
            delay = min(max(delay, 1.0), 30.0)
            print(
                f"Resend returned HTTP {response.status_code}; "
                f"retrying in {delay:g}s...",
                file=sys.stderr,
            )
            time.sleep(delay)

        raise RuntimeError("unreachable")

    def iter_paginated(
        self,
        path: str,
        *,
        limit: int,
    ) -> Iterator[dict[str, Any]]:
        after: str | None = None

        while True:
            params: dict[str, str | int] = {"limit": limit}
            if after:
                params["after"] = after
            payload = self.get(path, params=params)
            items = payload.get("data")
            if not isinstance(items, list):
                raise ValueError("Resend list response did not contain a data list")

            yield from items

            if not payload.get("has_more"):
                return
            if not items:
                raise ValueError("Resend returned has_more=true with an empty data list")
            after = items[-1].get("id")
            if not after:
                raise ValueError("Resend paginated item did not contain an id")


def iter_email_rows(
    client: ResendClient,
    *,
    since: date | None,
    page_size: int,
    run_timestamp: datetime,
) -> Iterator[dict[str, Any]]:
    for email in client.iter_paginated("/emails", limit=page_size):
        created_at = parse_timestamp(email["created_at"])
        if since and created_at.date() < since:
            return

        yield {
            "email_id": email["id"],
            "to_addresses": email.get("to") or [],
            "from_address": email.get("from"),
            "subject": email.get("subject"),
            "created_at": created_at.isoformat(),
            "last_event": email.get("last_event"),
            "backfilled_at": run_timestamp.isoformat(),
        }


def iter_contact_rows(
    client: ResendClient,
    *,
    page_size: int,
    run_timestamp: datetime,
) -> Iterator[dict[str, Any]]:
    snapshot_timestamp = run_timestamp.isoformat()

    for listed_contact in client.iter_paginated("/contacts", limit=page_size):
        contact_id = listed_contact["id"]
        contact = client.get(f"/contacts/{contact_id}")
        segments = list(
            client.iter_paginated(f"/contacts/{contact_id}/segments", limit=100)
        )
        topics = list(
            client.iter_paginated(f"/contacts/{contact_id}/topics", limit=100)
        )

        yield {
            "contact_id": contact_id,
            "email": contact["email"],
            "first_name": contact.get("first_name"),
            "last_name": contact.get("last_name"),
            "unsubscribed": contact["unsubscribed"],
            "created_at": parse_timestamp(contact["created_at"]).isoformat(),
            "properties": contact.get("properties") or {},
            "segment_ids": [segment["id"] for segment in segments],
            "topics": [
                {
                    "topic_id": topic["id"],
                    "name": topic.get("name"),
                    "description": topic.get("description"),
                    "subscription": topic.get("subscription"),
                }
                for topic in topics
            ],
            "backfilled_at": snapshot_timestamp,
            "last_seen_at": snapshot_timestamp,
        }


def merge_snapshot_rows(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    table_name: str,
    key_field: str,
    schema: list[bigquery.SchemaField],
    rows: list[dict[str, Any]],
    empty_message: str,
) -> None:
    if not rows:
        print(empty_message)
        return

    target_table = f"{project_id}.{dataset_id}.{table_name}"
    staging_table = (
        f"{project_id}.{dataset_id}._{table_name}_{uuid.uuid4().hex}"
    )
    load_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    field_names = [field.name for field in schema]
    update_fields = [field for field in field_names if field != key_field]
    update_sql = ",\n          ".join(
        f"{field} = source.{field}" for field in update_fields
    )
    insert_fields = ",\n          ".join(field_names)
    insert_values = ",\n          ".join(f"source.{field}" for field in field_names)

    try:
        print(f"Loading {len(rows)} rows into temporary table...")
        client.load_table_from_json(
            rows,
            staging_table,
            job_config=load_config,
        ).result()

        merge_sql = f"""
        MERGE `{target_table}` AS target
        USING `{staging_table}` AS source
        ON target.{key_field} = source.{key_field}
        WHEN MATCHED THEN UPDATE SET
          {update_sql}
        WHEN NOT MATCHED THEN INSERT (
          {insert_fields}
        ) VALUES (
          {insert_values}
        )
        """
        client.query(merge_sql).result()
    finally:
        client.delete_table(staging_table, not_found_ok=True)

    print(f"Merged {len(rows)} rows into {target_table}.")


def merge_email_rows(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    rows: list[dict[str, Any]],
) -> None:
    merge_snapshot_rows(
        client,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=EMAIL_BACKFILL_TABLE,
        key_field="email_id",
        schema=EMAIL_BACKFILL_SCHEMA,
        rows=rows,
        empty_message="No emails matched the requested range; nothing to load.",
    )


def merge_contact_rows(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    rows: list[dict[str, Any]],
) -> None:
    merge_snapshot_rows(
        client,
        project_id=project_id,
        dataset_id=dataset_id,
        table_name=CONTACT_BACKFILL_TABLE,
        key_field="contact_id",
        schema=CONTACT_BACKFILL_SCHEMA,
        rows=rows,
        empty_message="No contacts were returned; nothing to load.",
    )


def verify_bigquery_access(
    client: bigquery.Client,
    *,
    project_id: str,
    location: str | None,
) -> None:
    """Verify the active credentials can create BigQuery jobs."""
    try:
        client.query("SELECT 1 AS credential_check", location=location).result()
    except GoogleAPICallError as exc:
        credential_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        credential_source = (
            f"GOOGLE_APPLICATION_CREDENTIALS={credential_path}"
            if credential_path
            else "the default Application Default Credentials chain"
        )
        raise RuntimeError(
            "BigQuery credential preflight failed for project "
            f"{project_id} using {credential_source}. "
            "The active principal needs bigquery.jobs.create permission.\n"
            f"Google API error: {exc}\n"
            "If you intended to use credentials from "
            "`gcloud auth application-default login`, run:\n"
            "  env -u GOOGLE_APPLICATION_CREDENTIALS python scripts/backfill.py"
        ) from exc


def main() -> int:
    root_dir = Path(__file__).resolve().parent.parent
    load_env_file(root_dir / ".env")
    args = parse_args()

    api_key = os.environ.get("RESEND_API_KEY")
    if not api_key:
        print("RESEND_API_KEY must be set in .env or the environment.", file=sys.stderr)
        return 2
    if not args.project_id or args.project_id == "your-gcp-project-id":
        print("PROJECT_ID or --project-id is required.", file=sys.stderr)
        return 2

    try:
        client = bigquery.Client(project=args.project_id)
        verify_bigquery_access(
            client,
            project_id=args.project_id,
            location=os.environ.get("BQ_LOCATION"),
        )
    except (DefaultCredentialsError, RuntimeError) as exc:
        print(exc, file=sys.stderr)
        return 1

    run_timestamp = datetime.now(timezone.utc)
    with requests.Session() as session:
        resend = ResendClient(
            session,
            api_key,
            request_delay=args.request_delay,
        )

        if not args.contacts_only:
            email_rows = list(
                iter_email_rows(
                    resend,
                    since=args.since,
                    page_size=args.page_size,
                    run_timestamp=run_timestamp,
                )
            )
            merge_email_rows(
                client,
                project_id=args.project_id,
                dataset_id=args.dataset_id,
                rows=email_rows,
            )

        if not args.emails_only:
            contact_rows = list(
                iter_contact_rows(
                    resend,
                    page_size=args.page_size,
                    run_timestamp=run_timestamp,
                )
            )
            merge_contact_rows(
                client,
                project_id=args.project_id,
                dataset_id=args.dataset_id,
                rows=contact_rows,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
