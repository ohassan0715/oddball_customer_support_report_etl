# sample_run_local.sh
# Example script to run the Oddball ETL pipeline locally

set -e  # exit on error

# Ensure dependencies are installed
echo "Installing dependencies..."
pip install -r ../requirements.txt

# Run the local ETL pipeline
echo "Running local ETL pipeline..."
python ../scripts/oddball_de.py \
    --repo oddballteam/recruiting-challenges \
    --initial_path data-engineer/customer-support-report/data/initial \
    --delta_path data-engineer/customer-support-report/data/delta \
    --format csv \
    --months 202502,202503

echo "ETL pipeline completed. Outputs are available in the local ./output directory."
