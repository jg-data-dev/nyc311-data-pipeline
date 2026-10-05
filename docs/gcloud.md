# Google Cloud setup (gcloud and BigQuery)

This guide covers the Google Cloud side of the pipeline: installing the `gcloud` CLI, logging
in, checking that the login works, preparing the BigQuery datasets, and fixing the problems
most likely to come up.

Replace `YOUR_PROJECT_ID` throughout with your own Google Cloud project ID.

## What the pipeline needs

| Piece | What it is | Where it lives |
| --- | --- | --- |
| `gcloud` CLI (includes `bq`) | Command-line tools for logging in and inspecting BigQuery | Installed once per machine, outside this repository (for example `~/google-cloud-sdk`) |
| Application Default Credentials (ADC) | The login the ingest script and dbt use | `~/.config/gcloud/application_default_credentials.json` |
| Google Cloud project | Holds the BigQuery datasets | Google Cloud, with billing enabled |

Two points that are easy to miss:

- The pipeline never calls `gcloud` while it runs. The Python BigQuery client and dbt
  (`method: oauth` in `profiles.yml`) read the ADC file directly. `gcloud` is only needed to
  create that login and to inspect things.
- Credentials are stored in your home folder, not in the repository. One login covers every
  project folder on the machine, and there is no key file to commit by accident.

## 1. Install the gcloud CLI (once)

Install it outside any project folder so that moving or deleting a project never breaks it.
Follow the [official install guide](https://cloud.google.com/sdk/docs/install), or on macOS
with Homebrew:

```bash
brew install --cask google-cloud-sdk
```

Open a new terminal tab afterwards and confirm:

```bash
which gcloud
gcloud --version
```

## 2. Log in

There are two separate logins, and you need both:

```bash
gcloud auth login
gcloud auth application-default login
```

| Command | Used by |
| --- | --- |
| `gcloud auth login` | The `gcloud` and `bq` commands you type |
| `gcloud auth application-default login` | The ingest script, dbt, and the Airflow DAG |

Then set the default project:

```bash
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

## 3. Check that the login works

```bash
gcloud auth application-default print-access-token > /dev/null && echo "authenticated"
gcloud config get-value project
bq ls
```

`authenticated`, your project ID, and a list of datasets (possibly empty) mean everything is
in place. The Airflow DAG's first task, `check_environment`, performs the same connection
check and fails within seconds if the login is missing or expired.

## 4. Prepare the project

1. Enable billing on the project. The ingest step uses `MERGE`, which the free BigQuery
   sandbox does not allow.
2. Put the project ID in `.env` and `profiles.yml` (see the [README](../README.md) for the
   other places it is currently set).

The raw dataset (`nyc311_raw` by default) and the raw tables are created automatically by the
first ingest run. dbt creates the dataset named in `profiles.yml` on its first run.

## 5. Check for expiration settings

Datasets created in the BigQuery sandbox get a default 60-day expiration for tables and
partitions, and the setting remains after billing is enabled. The raw tables are partitioned
by `created_date`, so any row dated more than 60 days in the past is deleted as soon as it is
loaded. The ingest step still reports success.

Check each dataset the pipeline uses:

```bash
bq show --format=prettyjson YOUR_PROJECT_ID:nyc311_raw | grep -i expiration
```

No output means no expiration is set. If you see `defaultTableExpirationMs` or
`defaultPartitionExpirationMs`, remove the defaults and the expiration already applied to the
existing raw tables:

```bash
bq update --default_partition_expiration 0 --default_table_expiration 0 YOUR_PROJECT_ID:nyc311_raw
bq update --time_partitioning_expiration 0 YOUR_PROJECT_ID:nyc311_raw.raw_311_requests
bq update --time_partitioning_expiration 0 YOUR_PROJECT_ID:nyc311_raw.raw_311_requests_probe
```

Repeat the `bq show` check for the dbt dataset from `profiles.yml`; a table expiration there
removes the modeled tables 60 days after each build.

## 6. Inspect the data

Row counts per day in the raw table, for a date range you loaded:

```bash
bq query --use_legacy_sql=false \
'SELECT DATE(created_date) AS day, COUNT(*) AS row_count
 FROM `YOUR_PROJECT_ID.nyc311_raw.raw_311_requests`
 WHERE DATE(created_date) BETWEEN "2026-07-01" AND "2026-07-02"
 GROUP BY day ORDER BY day'
```

Tables in a dataset, and the details of one table:

```bash
bq ls YOUR_PROJECT_ID:nyc311_raw
bq show YOUR_PROJECT_ID:nyc311_raw.raw_311_requests
```

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| `gcloud: command not found` | The CLI is not on your `PATH`. Open a new terminal tab; if it persists, check the `google-cloud-sdk` lines in `~/.zshrc` point to where the SDK is installed. |
| `gcloud` stopped working after moving a folder | The SDK was inside the folder you moved. Update its path in `~/.zshrc` and open a new tab. Your login is unaffected. |
| `Reauthentication is needed` or `invalid_grant` | The login expired. Run `gcloud auth application-default login` again. |
| `bq` works but the pipeline cannot authenticate (or the reverse) | Only one of the two logins in step 2 was done. |
| `Raw table contains no rows` although the ingest log shows rows loaded | Partition expiration is deleting rows older than the limit. See step 5. |
| `Billing has not been enabled` or DML errors on `MERGE` | The project is still in the sandbox. Enable billing. |
| Data lands in an unexpected project | The ingest step reads `BIGQUERY_PROJECT_ID` (or the default in `ingest/ingest.py`), while the DAG checks use `GOOGLE_CLOUD_PROJECT`. Set both in `.env`. |
| A quota project warning after logging in | Run `gcloud auth application-default set-quota-project YOUR_PROJECT_ID`. |
