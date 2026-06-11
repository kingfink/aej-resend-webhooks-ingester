#!/usr/bin/env python3
"""Backfill Resend sent-email snapshots into BigQuery."""

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
from google.cloud import bigquery

API_URL = "https://api.resend.com/emails"
BACKFILL_TABLE = "resend_emails_backfill"
USER_AGENT = "resend-webhooks-cloudrun-backfill/1.0"
MAX_RETRIES = 5
REQUEST_TIMEOUT_SECONDS = 30

BACKFILL_SCHEMA = [
    bigquery.SchemaField("email_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("to_addresses", "STRING", mode="REPEATED"),
    bigquery.SchemaField("from_address", "STRING"),
    bigquery.SchemaField("subject", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("last_event", "STRING"),
    bigquery.SchemaField("backfilled_at", "TIMESTAMP", mode="REQUIRED"),
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
        description="Backfill latest Resend email status snapshots into BigQuery."
    )
    parser.add_argument(
        "--since",
        type=parse_date,
        help="Include emails created on or after this date (YYYY-MM-DD).",
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
        help="Seconds between page requests (default: 0.55, under 2 req/s).",
    )
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def request_page(
    session: requests.Session,
    api_key: str,
    *,
    after: str | None,
    limit: int,
) -> dict[str, Any]:
    params: dict[str, str | int] = {"limit": limit}
    if after:
        params["after"] = after

    headers = {
        "Authorization": f"Bearer {api_key}",
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }

    for attempt in range(MAX_RETRIES + 1):
        response = session.get(
            API_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        if response.status_code != 429 and response.status_code < 500:
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload.get("data"), list):
                raise ValueError("Resend response did not contain a data list")
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
            f"Resend returned HTTP {response.status_code}; retrying in {delay:g}s...",
            file=sys.stderr,
        )
        time.sleep(delay)

    raise RuntimeError("unreachable")


def iter_email_rows(
    session: requests.Session,
    api_key: str,
    *,
    since: date | None,
    page_size: int,
    request_delay: float,
    run_timestamp: datetime,
) -> Iterator[dict[str, Any]]:
    after: str | None = None

    while True:
        payload = request_page(session, api_key, after=after, limit=page_size)
        emails = payload["data"]
        reached_since_boundary = False

        for email in emails:
            created_at = parse_timestamp(email["created_at"])
            if since and created_at.date() < since:
                reached_since_boundary = True
                continue

            yield {
                "email_id": email["id"],
                "to_addresses": email.get("to") or [],
                "from_address": email.get("from"),
                "subject": email.get("subject"),
                "created_at": created_at.isoformat(),
                "last_event": email.get("last_event"),
                "backfilled_at": run_timestamp.isoformat(),
            }

        if reached_since_boundary or not payload.get("has_more"):
            return
        if not emails:
            raise ValueError("Resend returned has_more=true with an empty data list")

        after = emails[-1]["id"]
        time.sleep(max(request_delay, 0.0))


def merge_rows(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        print("No emails matched the requested range; nothing to load.")
        return

    target_table = f"{project_id}.{dataset_id}.{BACKFILL_TABLE}"
    staging_table = (
        f"{project_id}.{dataset_id}._resend_emails_backfill_{uuid.uuid4().hex}"
    )
    load_config = bigquery.LoadJobConfig(
        schema=BACKFILL_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

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
        ON target.email_id = source.email_id
        WHEN MATCHED THEN UPDATE SET
          to_addresses = source.to_addresses,
          from_address = source.from_address,
          subject = source.subject,
          created_at = source.created_at,
          last_event = source.last_event,
          backfilled_at = source.backfilled_at
        WHEN NOT MATCHED THEN INSERT (
          email_id,
          to_addresses,
          from_address,
          subject,
          created_at,
          last_event,
          backfilled_at
        ) VALUES (
          source.email_id,
          source.to_addresses,
          source.from_address,
          source.subject,
          source.created_at,
          source.last_event,
          source.backfilled_at
        )
        """
        client.query(merge_sql).result()
    finally:
        client.delete_table(staging_table, not_found_ok=True)

    print(f"Merged {len(rows)} rows into {target_table}.")


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

    run_timestamp = datetime.now(timezone.utc)
    with requests.Session() as session:
        rows = list(
            iter_email_rows(
                session,
                api_key,
                since=args.since,
                page_size=args.page_size,
                request_delay=args.request_delay,
                run_timestamp=run_timestamp,
            )
        )

    client = bigquery.Client(project=args.project_id)
    merge_rows(
        client,
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        rows=rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
