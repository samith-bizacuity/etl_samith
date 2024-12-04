# Imports
import subprocess
import os
import json
import datetime

# Set current directory as the working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Define scripts to be run
scripts = [
    "start_batch.py",
    "master_oracle_to_s3.py",
    "master_s3_to_stage.py",
    "master_stage_to_dw.py",
    "end_batch.py"
]

# Fetch ETL variables
def fetch_etl_variables():
    subprocess.run(["python", "get_etl_variables.py"], capture_output=True, text=True, check=True)

    with open('etl_variables.json', 'r') as f:
        etl_variables = json.load(f)

    return etl_variables

# Run all the necessary scripts
def run_scripts(scripts, etl_batch_no, etl_batch_date):
    for script in scripts:
        try:
            subprocess.run(["python", script, etl_batch_no, etl_batch_date], capture_output=True, text=True, check=True)
            print(f"{script} completed successfully")
        except subprocess.CalledProcessError as e:
            print(f"Error in {script}: {e.stderr}")
            break

# Main function
def main():
    start_time = datetime.datetime.now()
    print(f"ETL process started at: {start_time}")

    etl_variables = fetch_etl_variables()
    etl_batch_no = str(etl_variables['etl_batch_no'])
    etl_batch_date = etl_variables['etl_batch_date']

    run_scripts(scripts, etl_batch_no, etl_batch_date)

    end_time = datetime.datetime.now()
    print(f"ETL process ended at: {end_time}")

    print(f"\n ETL process for the batch no. {etl_batch_no} and batch date {etl_batch_date} completed successfully")
    print(f"\n Total time taken: {end_time - start_time}")

if __name__ == "__main__":
    main()