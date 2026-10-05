# Running the pipeline with Airflow

This guide covers a local Airflow 3 setup: where things live, how to register the DAG,
how to start Airflow and log in, and how to trigger and troubleshoot a run.

## How the pieces fit together

Airflow is installed separately from this repository. Three locations are involved:

| Location | What it is |
| --- | --- |
| `~/airflow-venv` | The Airflow installation (its own Python virtual environment) |
| `~/airflow` | `AIRFLOW_HOME`: Airflow's config (`airflow.cfg`), metadata database, logs, and `dags/` folder |
| `<this repo>/venv` | The project environment (dbt, BigQuery client, ingest dependencies) that the DAG's tasks call |

The paths `~/airflow-venv` and `~/airflow` are the ones used throughout this guide; substitute
your own if you installed Airflow elsewhere.

Airflow itself only schedules and runs shell commands. Every task `cd`s into this repository
and runs `venv/bin/python` or `venv/bin/dbt`, so Airflow's environment does not need dbt or the
BigQuery libraries, and the project environment does not need Airflow.

## 1. Prepare the project

Complete the Setup section of the [README](../README.md) first. The DAG's first task fails
unless all of these exist in the repository root:

- `venv/` with `requirements.txt` installed
- `.env` (copied from `.env.example`)
- `profiles.yml` (copied from `profiles.yml.example`)

You also need working Google credentials: `gcloud auth application-default login`.

## 2. Install Airflow (once)

Install Airflow 3 into its own virtual environment, following the
[official installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)
(it uses a constraints file, so copy the exact command from there).

```bash
python -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
# then run the pip install command from the official guide
```

## 3. Check the installation

```bash
source ~/airflow-venv/bin/activate

airflow version                             # must be 3.x
airflow info                                # shows AIRFLOW_HOME and config paths
airflow config get-value core dags_folder   # where Airflow looks for DAG files
```

When reading the Airflow documentation, select the version that `airflow version` prints.
Pages for Airflow 2.x do not match this DAG, which uses the Airflow 3 `airflow.sdk` API.

## 4. Register the DAG

Airflow only loads DAG files found in its DAGs folder. Rather than copying the file, create a
symlink so the repository stays the single source of truth:

```bash
mkdir -p ~/airflow/dags
ln -s "$(pwd)/dags/nyc311_bigquery.py" ~/airflow/dags/nyc311_prod.py   # run from the repo root
ls -l ~/airflow/dags                                                    # shows where each link points
```

The symlink's file name (`nyc311_prod.py`) is only a label for you. The name shown in the
Airflow UI is the `dag_id` set inside the file: **`nyc311_backfill_bigquery`**.

Confirm that it loaded:

```bash
airflow dags list | grep nyc311
airflow dags list-import-errors
```

## 5. Start Airflow and log in

```bash
source ~/airflow-venv/bin/activate
airflow standalone
```

Open http://localhost:8080 and log in as `admin`. The password is generated on first start,
printed in the `airflow standalone` output, and stored in:

```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```

Keep this password out of the repository (README, notes, and commits included). To get a new
one, stop Airflow, delete that file, and start Airflow again.

## 6. Trigger a run

The DAG has no schedule; it runs only when triggered, and it requires a date range.

| Config key | Required | Meaning |
| --- | --- | --- |
| `start_date`, `end_date` | Yes | Date range to ingest (`YYYY-MM-DD`) |
| `test` | No (default `false`) | `true` loads into `raw_311_requests_probe` instead of `raw_311_requests` |
| `reset_probe` | No (default `false`) | `true` deletes the probe table first; ignored unless `test` is `true` |

Start with a small probe run:

```json
{ "test": true, "reset_probe": true, "start_date": "2026-07-01", "end_date": "2026-07-02" }
```

Then run against the production raw table:

```json
{ "test": false, "start_date": "2026-07-01", "end_date": "2026-07-02" }
```

In the UI, open the DAG, choose **Trigger**, and paste the JSON as the run configuration.
From the command line:

```bash
airflow dags trigger nyc311_backfill_bigquery \
  --conf '{"test": true, "reset_probe": true, "start_date": "2026-07-01", "end_date": "2026-07-02"}'
```

## What the DAG does

Tasks run in this order, and each one must succeed before the next starts:

| Task | What it does |
| --- | --- |
| `check_environment` | Verifies `.env`, `venv`, `profiles.yml`, and `dbt_project.yml` exist, and that BigQuery is reachable |
| `reset_probe_raw` | Deletes the probe table when `test` and `reset_probe` are both `true`; otherwise does nothing |
| `ingest_backfill` | Runs `python -m ingest.ingest` for the date range into the selected raw table |
| `validate_raw` | Fails if the raw table is missing or empty |
| `dbt_debug` | Checks the dbt profile and connection |
| `dbt_run` | Builds staging, core, intermediate, and mart models |
| `dbt_test` | Runs the dbt tests |

Only one run is allowed at a time (`max_active_runs=1`).

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| DAG does not appear in the UI | Run `airflow dags list-import-errors`. Check that the symlink is inside the folder printed by `airflow config get-value core dags_folder` and that `ls -l` shows it pointing at an existing file. |
| Only one of two copies appears | Two DAG files share the same `dag_id`. Each must be unique; give the second copy a different ID (for example a `_dev` suffix). |
| `ERROR: Missing .env` | Copy `.env.example` to `.env` in the repository root. |
| `ERROR: Missing project Python` / `Missing project dbt` | Create `venv/` in the repository root and install `requirements.txt`. If you moved or renamed the repository folder, recreate `venv/` and the symlink, since both store absolute paths. |
| `ERROR: Missing dbt profiles.yml` | Copy `profiles.yml.example` to `profiles.yml`. |
| Credential or permission errors | Run `gcloud auth application-default login` and check the project in `.env`. |
| Ingest writes to the wrong project | The ingest step reads `BIGQUERY_PROJECT_ID` (or the default in `ingest/ingest.py`), not `GOOGLE_CLOUD_PROJECT`. Add `BIGQUERY_PROJECT_ID` to `.env` or change the default. |
| `ERROR: Missing required backfill date range` | The run was triggered without `start_date` and `end_date` in its configuration. |

To read a task's output, click the task in the run's grid view and open **Logs**; the files are
also under `~/airflow/logs`.
