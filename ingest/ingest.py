"""
Ingest NYC 311 records from the Socrata API into BigQuery.

Examples:
    python ingest_bigquery.py --target-date 2026-04-25
    python ingest_bigquery.py --start-date 2026-09-16 --end-date 2026-12-31
    python ingest_bigquery.py --max-pages 10
    python ingest_bigquery.py --target-date 2026-04-25 --max-pages 1
    # max-pages is ignored for date-filtered ingests

Authentication:
    This script uses Google Application Default Credentials (ADC).

    For local development:
        gcloud auth application-default login

Environment variables (all optional because defaults are provided):
    BIGQUERY_PROJECT_ID=nyc311-502315
    BIGQUERY_DATASET_ID=nyc311_raw
    BIGQUERY_LOCATION=US

Behavior:
    - Fetches NYC 311 records in pages of 1,000.
    - Loads each page into a temporary BigQuery merge_buffer table.
    - MERGEs the merge_buffer rows into the target table on unique_key.
    - Existing records are updated; new records are inserted.
    - The target table is created automatically if it does not exist.
    - The target table is partitioned daily by created_date.
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from typing import Any

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

PAGE_SIZE = 1000
DEFAULT_MAX_PAGES = 3

DEFAULT_PROJECT_ID = "nyc311-502315"
DEFAULT_DATASET_ID = "nyc311_raw"
DEFAULT_LOCATION = "US"
DEFAULT_TARGET_TABLE = "raw_311_requests"

ALLOWED_TARGET_TABLES = {
    "raw_311_requests",
    "raw_311_requests_probe",
}

# Keep the raw layer permissive: dates are typed, while most source attributes
# remain strings. JSON preserves nested source payloads without flattening them.
TABLE_SCHEMA = [
    bigquery.SchemaField("unique_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_date", "TIMESTAMP"),
    bigquery.SchemaField("closed_date", "TIMESTAMP"),
    bigquery.SchemaField("resolution_action_updated_date", "TIMESTAMP"),
    bigquery.SchemaField("agency", "STRING"),
    bigquery.SchemaField("agency_name", "STRING"),
    bigquery.SchemaField("complaint_type", "STRING"),
    bigquery.SchemaField("descriptor", "STRING"),
    bigquery.SchemaField("descriptor_2", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("resolution_description", "STRING"),
    bigquery.SchemaField("open_data_channel_type", "STRING"),
    bigquery.SchemaField("incident_address", "STRING"),
    bigquery.SchemaField("street_name", "STRING"),
    bigquery.SchemaField("cross_street_1", "STRING"),
    bigquery.SchemaField("cross_street_2", "STRING"),
    bigquery.SchemaField("intersection_street_1", "STRING"),
    bigquery.SchemaField("intersection_street_2", "STRING"),
    bigquery.SchemaField("landmark", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("borough", "STRING"),
    bigquery.SchemaField("incident_zip", "STRING"),
    bigquery.SchemaField("community_board", "STRING"),
    bigquery.SchemaField("council_district", "STRING"),
    bigquery.SchemaField("police_precinct", "STRING"),
    bigquery.SchemaField("park_borough", "STRING"),
    bigquery.SchemaField("park_facility_name", "STRING"),
    bigquery.SchemaField("taxi_pick_up_location", "STRING"),
    bigquery.SchemaField("bbl", "STRING"),
    bigquery.SchemaField("x_coordinate_state_plane", "STRING"),
    bigquery.SchemaField("y_coordinate_state_plane", "STRING"),
    bigquery.SchemaField("latitude", "STRING"),
    bigquery.SchemaField("longitude", "STRING"),
    bigquery.SchemaField("location", "JSON"),
    bigquery.SchemaField("raw_json", "JSON"),
]

COLUMN_NAMES = [field.name for field in TABLE_SCHEMA]


def validate_target_table(target_table: str) -> None:
    if target_table not in ALLOWED_TARGET_TABLES:
        raise ValueError(
            f"Invalid target table: {target_table}. "
            f"Allowed: {sorted(ALLOWED_TARGET_TABLES)}"
        )


def build_target_date_where_clause(target_date: str, offset_days: int) -> str:
    start_dt = datetime.fromisoformat(target_date)
    end_dt = start_dt + timedelta(days=offset_days)

    return (
        f"created_date >= '{start_dt:%Y-%m-%dT00:00:00}' "
        f"AND created_date < '{end_dt:%Y-%m-%dT00:00:00}'"
    )


def build_range_where_clause(start_date: str, end_date: str) -> str:
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)

    return (
        f"created_date >= '{start_dt:%Y-%m-%dT00:00:00}' "
        f"AND created_date <= '{end_dt:%Y-%m-%dT23:59:59}'"
    )


def fetch_page(*, offset: int, where_clause: str | None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$order": "created_date, unique_key",
    }

    if where_clause:
        params["$where"] = where_clause

    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    rows = response.json()
    if not isinstance(rows, list):
        raise TypeError(f"Expected Socrata to return a list, got {type(rows).__name__}")

    return rows


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    unique_key = row.get("unique_key")
    if unique_key is None:
        raise ValueError("Socrata row is missing required field: unique_key")

    return {
        "unique_key": str(unique_key),
        "created_date": row.get("created_date"),
        "closed_date": row.get("closed_date"),
        "resolution_action_updated_date": row.get(
            "resolution_action_updated_date"
        ),
        "agency": row.get("agency"),
        "agency_name": row.get("agency_name"),
        "complaint_type": row.get("complaint_type"),
        "descriptor": row.get("descriptor"),
        "descriptor_2": row.get("descriptor_2"),
        "status": row.get("status"),
        "resolution_description": row.get("resolution_description"),
        "open_data_channel_type": row.get("open_data_channel_type"),
        "incident_address": row.get("incident_address"),
        "street_name": row.get("street_name"),
        "cross_street_1": row.get("cross_street_1"),
        "cross_street_2": row.get("cross_street_2"),
        "intersection_street_1": row.get("intersection_street_1"),
        "intersection_street_2": row.get("intersection_street_2"),
        "landmark": row.get("landmark"),
        "city": row.get("city"),
        "borough": row.get("borough"),
        "incident_zip": row.get("incident_zip"),
        "community_board": row.get("community_board"),
        "council_district": row.get("council_district"),
        "police_precinct": row.get("police_precinct"),
        "park_borough": row.get("park_borough"),
        "park_facility_name": row.get("park_facility_name"),
        "taxi_pick_up_location": row.get("taxi_pick_up_location"),
        "bbl": row.get("bbl"),
        "x_coordinate_state_plane": row.get("x_coordinate_state_plane"),
        "y_coordinate_state_plane": row.get("y_coordinate_state_plane"),
        "latitude": row.get("latitude"),
        "longitude": row.get("longitude"),
        "location": row.get("location"),
        "raw_json": row,
    }


def ensure_dataset(
    client: bigquery.Client,
    *,
    project_id: str,
    dataset_id: str,
    location: str,
) -> None:
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        print(f"Created dataset: {project_id}.{dataset_id}")


def ensure_target_table(
    client: bigquery.Client,
    *,
    table_id: str,
) -> None:
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=TABLE_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_date",
    )
    client.create_table(table)
    print(f"Created partitioned target table: {table_id}")


def load_merge_buffer_table(
    client: bigquery.Client,
    *,
    merge_buffer_table_id: str,
    rows: list[dict[str, Any]],
    location: str,
) -> None:
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        rows,
        merge_buffer_table_id,
        job_config=job_config,
        location=location,
    )
    load_job.result()


def build_merge_sql(*, target_table_id: str, merge_buffer_table_id: str) -> str:
    update_assignments = ",\n        ".join(
        f"T.{column} = S.{column}"
        for column in COLUMN_NAMES
        if column != "unique_key"
    )
    insert_columns = ",\n        ".join(COLUMN_NAMES)
    insert_values = ",\n        ".join(f"S.{column}" for column in COLUMN_NAMES)

    # Deduplicate the merge_buffer page defensively before MERGE. BigQuery MERGE
    # requires at most one source row to match each target row.
    return f"""
MERGE `{target_table_id}` AS T
USING (
    SELECT * EXCEPT(_row_number)
    FROM (
        SELECT
            S.*,
            ROW_NUMBER() OVER (
                PARTITION BY unique_key
                ORDER BY created_date DESC
            ) AS _row_number
        FROM `{merge_buffer_table_id}` AS S
    )
    WHERE _row_number = 1
) AS S
ON T.unique_key = S.unique_key
WHEN MATCHED THEN
    UPDATE SET
        {update_assignments}
WHEN NOT MATCHED THEN
    INSERT (
        {insert_columns}
    )
    VALUES (
        {insert_values}
    )
"""


def upsert_rows(
    client: bigquery.Client,
    rows: list[dict[str, Any]],
    *,
    target_table_id: str,
    merge_buffer_table_id: str,
    location: str,
) -> int:
    if not rows:
        return 0

    normalized_rows = [normalize_row(row) for row in rows]

    load_merge_buffer_table(
        client,
        merge_buffer_table_id=merge_buffer_table_id,
        rows=normalized_rows,
        location=location,
    )

    merge_sql = build_merge_sql(
        target_table_id=target_table_id,
        merge_buffer_table_id=merge_buffer_table_id,
    )

    merge_job_config = bigquery.QueryJobConfig(
        maximum_bytes_billed=100_000_000  # 100 MB
    )

    merge_job = client.query(
        merge_sql,
        location=location,
        job_config=merge_job_config,
    )
    merge_job.result()

    return len(normalized_rows)


def run_ingest(
    *,
    project_id: str,
    dataset_id: str,
    location: str,
    target_table: str,
    where_clause: str | None,
    max_pages: int | None,
) -> int:
    validate_target_table(target_table)

    client = bigquery.Client(project=project_id, location=location)

    ensure_dataset(
        client,
        project_id=project_id,
        dataset_id=dataset_id,
        location=location,
    )

    target_table_id = f"{project_id}.{dataset_id}.{target_table}"
    merge_buffer_table_id = f"{project_id}.{dataset_id}._merge_buffer_{target_table}"

    ensure_target_table(client, table_id=target_table_id)

    total = 0
    page_num = 0

    print(f"BigQuery target: {target_table_id}")

    if where_clause:
        print(f"WHERE: {where_clause}")
        print("Date-filtered ingest: max_pages ignored.")
    else:
        print(f"No date filter. max_pages={max_pages}")

    while True:
        if where_clause is None and max_pages is not None and page_num >= max_pages:
            break

        offset = page_num * PAGE_SIZE
        print(f"Fetching page {page_num + 1}, offset={offset} ...")

        rows = fetch_page(offset=offset, where_clause=where_clause)

        if not rows:
            print("No more rows returned.")
            break

        loaded = upsert_rows(
            client,
            rows,
            target_table_id=target_table_id,
            merge_buffer_table_id=merge_buffer_table_id,
            location=location,
        )
        total += loaded

        print(f"Inserted/updated {loaded} rows.")

        page_num += 1

    print(f"Ingest finished. Total inserted/updated: {total}")
    return total


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project-id",
        default=os.getenv("BIGQUERY_PROJECT_ID", DEFAULT_PROJECT_ID),
    )
    parser.add_argument(
        "--dataset-id",
        default=os.getenv("BIGQUERY_DATASET_ID", DEFAULT_DATASET_ID),
    )
    parser.add_argument(
        "--location",
        default=os.getenv("BIGQUERY_LOCATION", DEFAULT_LOCATION),
    )

    parser.add_argument(
        "--target-table",
        default=DEFAULT_TARGET_TABLE,
        choices=sorted(ALLOWED_TARGET_TABLES),
    )

    parser.add_argument("--target-date")

    parser.add_argument(
        "--offset-days",
        type=int,
        default=1,
    )

    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    where_clause = None

    if args.target_date:
        where_clause = build_target_date_where_clause(
            target_date=args.target_date,
            offset_days=args.offset_days,
        )

    elif args.start_date and args.end_date:
        where_clause = build_range_where_clause(
            start_date=args.start_date,
            end_date=args.end_date,
        )

    elif args.start_date or args.end_date:
        raise ValueError("Use both --start-date and --end-date, or neither.")

    run_ingest(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        location=args.location,
        target_table=args.target_table,
        where_clause=where_clause,
        max_pages=args.max_pages,
    )


if __name__ == "__main__":
    main()