# NYC 311 Data Pipeline

An end-to-end pipeline that ingests NYC 311 service requests from the NYC Open Data
(Socrata) API into BigQuery, models them with dbt, and orchestrates the run with Airflow.

```
                    Git repository
        ┌─────────────────────────────────┐
        │ Python code │ DAGs │ dbt models │
        └─────────────────────────────────┘
                         │ deployed to
                         ▼
                  Airflow environment
             scheduler + task workers
                  │               │
          runs Python          runs dbt
                  │               │
                  ▼               ▼
            NYC 311 API       dbt-bigquery
                  │               │
                  └──────┬────────┘
                         ▼
                      BigQuery
        raw tables → staging tables → marts
```

## Repository layout

| Path | Contents |
| --- | --- |
| `ingest/ingest.py` | Pages through the 311 API and MERGEs rows into a day-partitioned BigQuery raw table on `unique_key` |
| `models/staging` | Source definition and cleaned staging model |
| `models/core` | Dimensions and complaint fact tables |
| `models/intermediate` | Data-quality flags |
| `models/marts` | Daily, monthly, and borough complaint-mix metrics |
| `tests/` | Singular and generic dbt tests |
| `dags/nyc311_bigquery.py` | Airflow DAG: ingest → validate raw → dbt debug → dbt run → dbt test |

## Prerequisites

- Python 3.12
- A Google Cloud project with BigQuery enabled and billing turned on (the ingest uses `MERGE`)
- The `gcloud` CLI
- Airflow 3, installed in its own environment (only needed for the orchestrated run)

## Setup

1. Create the project virtual environment. The DAG expects it at `venv/` in the repo root.

   ```bash
   python -m venv venv
   source venv/bin/activate
   python -m pip install -r requirements.txt
   ```

2. Authenticate with Application Default Credentials and set your project.

   ```bash
   gcloud auth application-default login
   gcloud config set project your-gcp-project-id
   ```

3. Create your local config files from the templates and fill in your project and datasets.

   ```bash
   cp .env.example .env
   cp profiles.yml.example profiles.yml
   ```

4. Point the code at your project. The project ID is currently also set in
   `models/staging/sources.yml` (`database`) and as the default in `ingest/ingest.py`;
   change it there, or pass `--project-id` / set `BIGQUERY_PROJECT_ID` for the ingest.

## Run manually

```bash
source venv/bin/activate
set -a; source .env; set +a

# Ingest a date range into the raw table (created automatically if missing)
python -m ingest.ingest --project-id "$GOOGLE_CLOUD_PROJECT" \
    --start-date 2026-07-01 --end-date 2026-07-02

# Build and test the models
dbt debug
dbt run
dbt test
```

## Run with Airflow

Full instructions (installation check, registering the DAG, logging in, triggering, and
troubleshooting) are in [docs/airflow.md](docs/airflow.md). In short:

1. Symlink `dags/nyc311_bigquery.py` into your Airflow DAGs folder.
2. Start Airflow (`airflow standalone` for a local run).
3. Trigger the `nyc311_backfill_bigquery` DAG with a config:

   ```json
   { "test": true, "reset_probe": true, "start_date": "2026-07-01", "end_date": "2026-07-02" }
   ```

   - `test: true` loads into the `raw_311_requests_probe` table instead of `raw_311_requests`.
   - `reset_probe: true` clears the probe table before loading.
   - `start_date` and `end_date` are required.
