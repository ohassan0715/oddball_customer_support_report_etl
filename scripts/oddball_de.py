import argparse
import logging
import re
import requests
import pandas as pd
import pytz
from pathlib import Path
from pyspark.sql import SparkSession

# ----------------------------
# Logging Setup
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ----------------------------
# Helpers
# ----------------------------
def load_csvs_from_github(repo: str, path: str) -> dict:
    """Load all CSV files from a GitHub repo folder into a dict of DataFrames."""
    api_url = f"https://api.github.com/repos/{repo}/contents/{path}"
    resp = requests.get(api_url)
    resp.raise_for_status()
    files = resp.json()

    dfs = {}
    for f in files:
        if f["name"].endswith(".csv"):
            df = pd.read_csv(f["download_url"])
            key = f["name"].replace(".csv", "")
            dfs[key] = df
            logging.info(f"Loaded {f['name']} ({len(df)} rows)")
    return dfs


def apply_delta(base_df: pd.DataFrame, delta_df: pd.DataFrame, key_col: str) -> pd.DataFrame:
    """Apply inserts/updates/deletes from a single month's delta."""
    if "action" not in delta_df.columns:
        raise ValueError("Delta file must contain an 'action' column")

    # Normalize action
    delta_df["action"] = delta_df["action"].str.strip().str.lower()

    # Handle deletes
    deletes = delta_df[delta_df["action"] == "delete"][key_col].unique()
    base_df = base_df[~base_df[key_col].isin(deletes)]

    # Inserts/updates (overwrite existing rows)
    upserts = delta_df[delta_df["action"].isin(["add", "update"])].drop(columns=["action"])
    if not upserts.empty:
        base_df = base_df[~base_df[key_col].isin(upserts[key_col])]
        base_df = pd.concat([base_df, upserts], ignore_index=True)

    return base_df


def convert_interaction_timestamps_to_est(df: pd.DataFrame) -> pd.DataFrame:
    """Convert interaction timestamp columns from UTC to EST."""
    est = pytz.timezone("US/Eastern")
    ts_cols = ["timestamp", "interaction_start", "agent_resolution_timestamp", "interaction_end"]

    for col in ts_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)       # parse as UTC
            df[col] = df[col].dt.tz_convert(est)             # convert to EST
            df[col] = df[col].dt.tz_localize(None)           # remove timezone info

    return df


def validate_df(df: pd.DataFrame, key_col: str, name: str):
    """Run simple validation checks."""
    if df[key_col].isna().any():
        logging.warning(f"[{name}] Null values found in {key_col}")
    if df[key_col].duplicated().any():
        logging.warning(f"[{name}] Duplicate IDs found in {key_col}")
    logging.info(f"[{name}] Validated {len(df)} records")


def save_output(df: pd.DataFrame, name: str, fmt: str, outdir: str = "output"):
    """Save DataFrame to disk in specified format."""
    Path(outdir).mkdir(parents=True, exist_ok=True)
    path = Path(outdir) / f"{name}.{fmt}"

    if fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records", indent=2)
    elif fmt == "parquet":
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    logging.info(f"Saved {name} → {path}")


def save_to_catalog(clean_dfs: dict, catalog_db: str, mode: str = "overwrite"):
    """Save final DataFrames to Databricks Unity Catalog."""
    spark = SparkSession.builder.getOrCreate()
    for name, pdf in clean_dfs.items():
        sdf = spark.createDataFrame(pdf)
        table_name = f"{catalog_db}.{name}"
        sdf.write.mode(mode).saveAsTable(table_name)
        logging.info(f"Saved {name} to catalog table {table_name}")


# ----------------------------
# Main Pipeline
# ----------------------------
def run_pipeline(repo, initial_path, delta_path, months: list[str], fmt: str, catalog_db: str = None, all_delta_dfs: dict = None):
    logging.info("Starting ETL pipeline...")

    # Load initial state
    initial_dfs = load_csvs_from_github(repo, initial_path)
    state = {name: df.copy() for name, df in initial_dfs.items()}

    # Process deltas month-by-month
    for month in sorted(months):
        month_dfs = {name: df for name, df in all_delta_dfs.items() if name.endswith(f"_{month}")}

        if not month_dfs:
            logging.warning(f"No deltas found for {month}, skipping")
            continue

        for base_name, base_df in state.items():
            key_col = base_df.columns[0]  # first column as PK
            delta_key = f"{base_name}_delta_{month}"
            if delta_key in month_dfs:
                logging.info(f"Applying {month} delta to {base_name}")
                state[base_name] = apply_delta(base_df, month_dfs[delta_key], key_col)
                validate_df(state[base_name], key_col, base_name)

    # Save final outputs, sorted by ID and convert timestamps
    for name, df in state.items():
        key_col = df.columns[0]  # first column as PK
        df = df.sort_values(by=key_col).reset_index(drop=True)

        # Convert interaction timestamps only for interactions table
        if name == "interactions":
            df = convert_interaction_timestamps_to_est(df)

        save_output(df, name, fmt)

    # Optionally push to catalog
    if catalog_db:
        save_to_catalog(state, catalog_db)

    logging.info("ETL pipeline finished successfully")
    return state


# ----------------------------
# CLI Entrypoint
# ----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process initial + delta files from GitHub")
    parser.add_argument("--repo", type=str, default="oddballteam/recruiting-challenges")
    parser.add_argument("--initial_path", type=str, default="data-engineer/customer-support-report/data/initial")
    parser.add_argument("--delta_path", type=str, default="data-engineer/customer-support-report/data/delta")
    parser.add_argument("--months", type=str, help="Comma-separated months (e.g. 202502,202503)")
    parser.add_argument("--format", type=str, default="csv", choices=["csv", "json", "parquet"])
    parser.add_argument("--catalog_db", type=str, help="Optional catalog schema (e.g. workspace.oddball_custom)")
    args = parser.parse_args()

    # Load all delta CSVs once
    all_delta_dfs = load_csvs_from_github(args.repo, args.delta_path)

    if args.months:
        months = args.months.split(",")
    else:
        # Extract all months from delta filenames dynamically
        months = sorted(
            {re.search(r"(\d{6})$", fname).group(1)
             for fname in all_delta_dfs.keys()
             if re.search(r"(\d{6})$", fname)}
        )

    logging.info(f"Processing months: {months}")

    run_pipeline(
        repo=args.repo,
        initial_path=args.initial_path,
        delta_path=args.delta_path,
        months=months,
        fmt=args.format,
        catalog_db=args.catalog_db,
        all_delta_dfs=all_delta_dfs
    )
