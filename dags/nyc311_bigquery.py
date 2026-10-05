"""
NYC 311 v4 BigQuery backfill pipeline.

Trigger examples
----------------

Probe backfill and reset the probe table first:

{
  "test": true,
  "reset_probe": true,
  "start_date": "2026-07-01",
  "end_date": "2026-07-02"
}

Probe backfill without resetting existing probe data:

{
  "test": true,
  "reset_probe": false,
  "start_date": "2026-07-01",
  "end_date": "2026-07-02"
}

Production backfill:

{
  "test": false,
  "start_date": "2026-07-01",
  "end_date": "2026-07-02"
}
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


# This file lives at v4_cloud/dags/nyc311_bigquery.py.
V4_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = V4_ROOT / "venv" / "bin" / "python"
PROJECT_DBT = V4_ROOT / "venv" / "bin" / "dbt"


def render_bash(script: str) -> str:
    """Insert absolute project paths without using nested Python f-strings."""
    return (
        script.replace("__V4_ROOT__", str(V4_ROOT))
        .replace("__PROJECT_PYTHON__", str(PROJECT_PYTHON))
        .replace("__PROJECT_DBT__", str(PROJECT_DBT))
    )


LOAD_PROJECT_ENV_BASH = r'''
if [ ! -f .env ]; then
    echo "ERROR: Missing .env in $(pwd)"
    exit 1
fi

set -a
source .env
set +a

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-nyc311-502315}"
RAW_DATASET="${NYC311_RAW_DATASET:-nyc311_raw}"

export PROJECT_ID
export RAW_DATASET
export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"

echo "PROJECT_ID=$PROJECT_ID"
echo "RAW_DATASET=$RAW_DATASET"
'''


RAW_TABLE_BASH = r'''
TEST_MODE="{{ dag_run.conf.get('test', false) if dag_run else false }}"

case "$TEST_MODE" in
    True|true|1)
        RAW_TABLE="raw_311_requests_probe"
        ;;
    False|false|0|"")
        RAW_TABLE="raw_311_requests"
        ;;
    *)
        echo "ERROR: Invalid test value: $TEST_MODE"
        echo "Use true or false."
        exit 1
        ;;
esac

export RAW_TABLE

echo "TEST_MODE=$TEST_MODE"
echo "RAW_TABLE=$RAW_TABLE"
'''


RESET_PROBE_BASH = r'''
RESET_PROBE="{{ dag_run.conf.get('reset_probe', false) if dag_run else false }}"

case "$RESET_PROBE" in
    True|true|1)
        RESET_PROBE="true"
        ;;
    False|false|0|"")
        RESET_PROBE="false"
        ;;
    *)
        echo "ERROR: Invalid reset_probe value: $RESET_PROBE"
        echo "Use true or false."
        exit 1
        ;;
esac

export RESET_PROBE

echo "RESET_PROBE=$RESET_PROBE"
'''


DATE_RANGE_BASH = r'''
START_DATE="{{ dag_run.conf.get('start_date') if dag_run and dag_run.conf.get('start_date') else '' }}"
END_DATE="{{ dag_run.conf.get('end_date') if dag_run and dag_run.conf.get('end_date') else '' }}"

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
    echo "ERROR: Missing required backfill date range."
    echo "Provide both start_date and end_date in dag_run.conf."
    exit 1
fi

export START_DATE
export END_DATE

echo "START_DATE=$START_DATE"
echo "END_DATE=$END_DATE"
'''


with DAG(
    dag_id="nyc311_backfill_bigquery",
    description=(
        "Local Airflow pipeline that ingests NYC 311 data into BigQuery "
        "and then runs dbt models and tests."
    ),
    start_date=pendulum.datetime(2026, 7, 1, tz="America/Los_Angeles"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["nyc311", "v4", "bigquery", "dbt", "backfill"],
) as dag:

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + r'''

if [ ! -x "__PROJECT_PYTHON__" ]; then
    echo "ERROR: Missing project Python: __PROJECT_PYTHON__"
    exit 1
fi

if [ ! -x "__PROJECT_DBT__" ]; then
    echo "ERROR: Missing project dbt: __PROJECT_DBT__"
    exit 1
fi

if [ ! -f "profiles.yml" ]; then
    echo "ERROR: Missing dbt profiles.yml in __V4_ROOT__"
    exit 1
fi

if [ ! -f "dbt_project.yml" ]; then
    echo "ERROR: Missing dbt_project.yml in __V4_ROOT__"
    exit 1
fi

"__PROJECT_PYTHON__" --version
"__PROJECT_DBT__" --version

"__PROJECT_PYTHON__" - <<'PYCODE'
import os

import google.auth
from google.cloud import bigquery

project_id = os.environ["PROJECT_ID"]
credentials, detected_project = google.auth.default()

print("Credential type:", type(credentials).__name__)
print("Project from environment:", project_id)
print("Project detected from credentials:", detected_project)

client = bigquery.Client(project=project_id)
list(client.list_datasets(max_results=1))
print("BigQuery connection check passed.")
PYCODE
'''
        ),
        execution_timeout=timedelta(minutes=10),
    )

    reset_probe_raw = BashOperator(
        task_id="reset_probe_raw",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + RESET_PROBE_BASH + r'''

if [ "$RAW_TABLE" != "raw_311_requests_probe" ]; then
    echo "Production mode: the production raw table will not be reset."
    exit 0
fi

if [ "$RESET_PROBE" != "true" ]; then
    echo "Probe reset not requested."
    exit 0
fi

"__PROJECT_PYTHON__" - <<'PYCODE'
import os

from google.cloud import bigquery

project_id = os.environ["PROJECT_ID"]
dataset_id = os.environ["RAW_DATASET"]
table_name = os.environ["RAW_TABLE"]
table_id = f"{project_id}.{dataset_id}.{table_name}"

client = bigquery.Client(project=project_id)
client.delete_table(table_id, not_found_ok=True)
print(f"Deleted probe table if present: {table_id}")
PYCODE
'''
        ),
        execution_timeout=timedelta(minutes=10),
    )

    ingest_backfill = BashOperator(
        task_id="ingest_backfill",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + DATE_RANGE_BASH + r'''

echo "Ingesting into $PROJECT_ID.$RAW_DATASET.$RAW_TABLE"

"__PROJECT_PYTHON__" -m ingest.ingest \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --target-table "$RAW_TABLE"
'''
        ),
        execution_timeout=timedelta(hours=23),
        retries=0,
    )

    validate_raw = BashOperator(
        task_id="validate_raw",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + r'''

"__PROJECT_PYTHON__" - <<'PYCODE'
import os

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

project_id = os.environ["PROJECT_ID"]
dataset_id = os.environ["RAW_DATASET"]
table_name = os.environ["RAW_TABLE"]
table_id = f"{project_id}.{dataset_id}.{table_name}"

client = bigquery.Client(project=project_id)

try:
    table = client.get_table(table_id)
except NotFound as exc:
    raise RuntimeError(f"Expected raw table does not exist: {table_id}") from exc

query = f"SELECT COUNT(*) AS row_count FROM `{table_id}`"
row_count = next(client.query(query).result()).row_count

print(f"Table: {table_id}")
print(f"Schema fields: {len(table.schema)}")
print(f"Rows: {row_count:,}")

if row_count == 0:
    raise RuntimeError(f"Raw table contains no rows: {table_id}")

print("Raw table validation passed.")
PYCODE
'''
        ),
        execution_timeout=timedelta(minutes=30),
    )

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + r'''

NYC311_RAW_TABLE="$RAW_TABLE" \
"__PROJECT_DBT__" debug \
    --project-dir "__V4_ROOT__" \
    --profiles-dir "__V4_ROOT__"
'''
        ),
        execution_timeout=timedelta(minutes=10),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + r'''

NYC311_RAW_TABLE="$RAW_TABLE" \
"__PROJECT_DBT__" run \
    --project-dir "__V4_ROOT__" \
    --profiles-dir "__V4_ROOT__"
'''
        ),
        execution_timeout=timedelta(hours=2),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=render_bash(
            r'''
set -euo pipefail
cd "__V4_ROOT__"

''' + LOAD_PROJECT_ENV_BASH + RAW_TABLE_BASH + r'''

NYC311_RAW_TABLE="$RAW_TABLE" \
"__PROJECT_DBT__" test \
    --project-dir "__V4_ROOT__" \
    --profiles-dir "__V4_ROOT__"
'''
        ),
        execution_timeout=timedelta(hours=1),
    )

    (
        check_environment
        >> reset_probe_raw
        >> ingest_backfill
        >> validate_raw
        >> dbt_debug
        >> dbt_run
        >> dbt_test
    )
