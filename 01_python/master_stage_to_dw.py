# Imports
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

# Set current directory as the working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# List of scripts
dw_topology = {
    'stage_to_dw/offices.py' : ['stage_to_dw/employees.py'],
    'stage_to_dw/employees.py' : ['stage_to_dw/customers.py'],
    'stage_to_dw/customers.py' : ['stage_to_dw/orders.py', 'stage_to_dw/payments.py', 'stage_to_dw/customer_history.py'],
    'stage_to_dw/payments.py' : [],
    'stage_to_dw/customer_history.py' : [],
    'stage_to_dw/orders.py' : ['stage_to_dw/productlines.py'],
    'stage_to_dw/productlines.py' : ['stage_to_dw/products.py'],
    'stage_to_dw/products.py' : ['stage_to_dw/product_history.py', 'stage_to_dw/orderdetails.py'],
    'stage_to_dw/product_history.py' : [],
    'stage_to_dw/orderdetails.py' : ['stage_to_dw/dps.py', 'stage_to_dw/dcs.py'],
    'stage_to_dw/dps.py' : ['stage_to_dw/mps.py'],
    'stage_to_dw/mps.py' : [],
    'stage_to_dw/dcs.py' : ['stage_to_dw/mcs.py'],
    'stage_to_dw/mcs.py' : []
}

# Function to run script
def run_script(script_name, etl_batch_no, etl_batch_date):
    try:
        subprocess.run(
            ["python", script_name, etl_batch_no, etl_batch_date], capture_output=True, text=True, check=True
        )

        print(f"{script_name} completed successfully")

        # Run dependent scripts
        child_scripts = dw_topology[script_name]

        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(run_script, script, etl_batch_no, etl_batch_date): script for script in child_scripts}
            
            for future in as_completed(futures):
                script = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"{script} generated an exception: {e}")
            return True
            
    except subprocess.CalledProcessError as e:
        print(f"Error in {script_name}: {e.stderr}")
        return False


def main():
    # Fetch ETL variables
    etl_batch_no = sys.argv[1]
    etl_batch_date = sys.argv[2]

    # First script to run
    first_script = 'stage_to_dw/offices.py'

    # Run export scripts 
    print("Running export scripts...")
    run_script(first_script, etl_batch_no, etl_batch_date)


if __name__ == "__main__":
    main()