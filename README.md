# Customer Support ETL Pipeline
This repository contains two ETL pipelines for processing customer support data stored as CSV files in GitHub.
The pipelines apply deltas month-by-month, validate records, convert timestamps from UTC to EST, and output results either as local files or into Databricks Unity Catalog.

## 📂 Project Structure
oddbll_customer_support_report_etl/\
 └─ examples/\
 $$\space$$ $$\space$$ └─ sample_run_local.sh _(Example local run command)_\
 $$\space$$ $$\space$$ └─ databricks_job_config.json _(Example Databricks job/task config)_\
 └─ scripts/\
 $$\space$$ $$\space$$ └─ oddball_de.py _(Local CSV/JSON/Parquet ETL pipeline)_\
 $$\space$$ $$\space$$ └─ oddball_databricks.py _(Databricks ETL pipeline)_\
 └─ Oddball-ETL-Pipeline.pptx\
 └─ README.md\
 └─ requirements.txt\
 └─ support_report.sql

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

## 📝 Report Solution
This repository also includes the Customer Support Report Exercise, which answers three key questions:
1) **Question 1: What were the total number of interactions handled by each contact center in Q1 2025?**\
Solution:\
After extracting, and validating, the quarter of the month field created in the customer_support_report view; this was the output for Q1 2025 interactions by Contact Center:
  * Atlanta GA SE:	8
  * Richmond VA E:	7
  * Boston MA NE:	13
    
2) **Which month (Jan, Feb, or Mar) had the highest total interaction volume?**\
Solution:\
After applying a RANK() window function and ordering by total interactions,\
**_February was the top ranked month with 10 interactions._**

3) **Which contact center had the longest average phone call duration (`total_call_duration`)?**\
Solution:\
Using the RANK() function and dividing the total call duration over total calls,\
**_Boston was the top ranked contact center with an average 12.72 minute call duration._**
    * Why might this be the case based on the interactions data?\
      The initial analysis (timediff formula) discovered Boston was the only contact center to have a time difference between the agent resolution time and the interaction end time.\
      Since these were also the only records with a value for "Satisfaction Rating,"\
      **_this concludes that the time difference was allocated to a survey being completed after the interaction was resolved._**
    
    * What approach would you recommend to measure agent work time more accurately?\
      Solution: There is a coaching opportunity for the other contact centers to conduct surveys like Boston does;
      **_and using the resolution time as the end time for calculating agent work time would be more effective._**

Notes:
* The report solution leverages the same ETL pipeline logic to ensure consistent results.
* Outputs can be viewed as CSV, JSON, or Parquet files locally, or queried directly in Databricks Unity Catalog tables.

## 📌 Notes
* Use oddball_de.py for local testing, CSV/JSON/Parquet outputs.
* Use oddball_databricks.py for Databricks ingestion into Unity Catalog.
* Both scripts use the same core logic for consistency.
