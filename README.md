# Customer Support ETL Pipeline
This repository contains two ETL pipelines for processing customer support data stored as CSV files in GitHub.
The pipelines apply deltas month-by-month, validate records, convert timestamps from UTC to EST, and output results either as local files or into Databricks Unity Catalog.

## 📂 Project Structure
oddbll_customer_support_report_etl/\
|__ README.md\
|__ requirements.txt\
|__ scripts/\
|----> oddball_de.py _(Local CSV/JSON/Parquet ETL pipeline)_\
|----> oddball_databricks.py _(Databricks ETL pipeline)_\
|__ examples/\
|----> sample_run_local.sh _(Example local run command)_\
|----> databricks_job_config.json _(Example Databricks job/task config)_

## 🚀 Usage
**Option 1: Run Locally (produce CSV/JSON/Parquet)**

1) Install dependencies:
```
pip install -r requirements.txt
```   
2) Run the pipeline:
```
python scripts/oddball_de.py \
    --repo oddballteam/recruiting-challenges \
    --initial_path data-engineer/customer-support-report/data/initial \
    --delta_path data-engineer/customer-support-report/data/delta \
    --format csv \
    --months 202502,202503
```

**Option 2: Run in Databricks (Unity Catalog)**

1) Import scripts/oddball_databricks.py as a notebook into Databricks.
2) Configure widgets:
* repo → GitHub repo (default: oddballteam/recruiting-challenges)
* initial_path → Path to initial data (default provided)
* delta_path → Path to delta data (default provided)
* months → Comma-separated list of months (optional)
* format → csv, json, or parquet (default: csv)
* catalog_db → Unity Catalog schema (e.g., workspace.oddball_custom)
3) Run interactively or schedule as a Databricks Job.
* Outputs are written to /dbfs/tmp/output and/or Unity Catalog.

## 🕒 Timestamp Conversion
* All system-generated timestamps in the interactions dataset are in UTC.
* The pipeline automatically converts these fields to Eastern Time (EST/EDT):
    * timestamp
    * interaction_start
    * agent_resolution_timestamp
    * interaction_end

## ✅ Validation Rules
* Ensures no nulls in primary keys.
* Logs warnings if duplicates are found.
* Applies deltas in order (delete, update, add).

## 📌 Notes
* Use oddball_de.py for local testing, CSV/JSON/Parquet outputs.
* Use oddball_databricks.py for Databricks ingestion into Unity Catalog.
* Both scripts use the same core logic for consistency.
