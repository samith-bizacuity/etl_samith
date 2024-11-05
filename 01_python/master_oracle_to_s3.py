import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json

# Set current directory as the working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# List of scripts
export_scripts = [
    "oracle_to_s3/offices.py",
    "oracle_to_s3/customers.py",
    "oracle_to_s3/employees.py",
    "oracle_to_s3/payments.py",
    "oracle_to_s3/products.py",
    "oracle_to_s3/orders.py",
    "oracle_to_s3/productlines.py",
    "oracle_to_s3/orderdetails.py"
]

# Function to run a single script
def run_script(script_name, etl_batch_date):
    try:
        subprocess.run(
            ["python", script_name, etl_batch_date], capture_output=True, text=True, check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error in {script_name}: {e.stderr}")
        return False

# Main function to execute all export scripts in parallel
def run_export_scripts(etl_batch_date):
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_script, script, etl_batch_date): script for script in export_scripts}
        
        for future in as_completed(futures):
            script = futures[future]
            try:
                future.result()
                print(f"{script} completed successfully")
            except Exception as e:
                print(f"{script} generated an exception: {e}")

if __name__ == "__main__":
    
    # Fetch ETL variables
    subprocess.run(["python", "get_etl_variables.py"], capture_output=True, text=True, check=True)

    with open('etl_variables.json', 'r') as f:
        etl_variables = json.load(f)
        etl_batch_date = etl_variables['etl_batch_date']

    # Run export scripts in parallel
    print("Running export scripts in parallel...")
    run_export_scripts(etl_batch_date)