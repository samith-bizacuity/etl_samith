import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Set current directory as the working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# List of scripts
setup_scripts = ["etl_batch_update.py", "etl_db_link.py"]
export_scripts = [
    "offices.py",
    "customers.py",
    "employees.py",
    "payments.py",
    "products.py",
    "orders.py",
    "productlines.py",
    "orderdetails.py"
]

# Function to run a single script
def run_script(script_name):
    try:
        subprocess.run(
            ["python", script_name], capture_output=True, text=True, check=True
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error in {script_name}: {e.stderr}")
        return False

# Main function to execute all export scripts in parallel
def run_export_scripts():
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(run_script, script): script for script in export_scripts}
        
        for future in as_completed(futures):
            script = futures[future]
            try:
                future.result()
                print(f"{script} completed successfully")
            except Exception as e:
                print(f"{script} generated an exception: {e}")

if __name__ == "__main__":
    # Run setup scripts sequentially
    print("Running setup scripts...")
    for setup_script in setup_scripts:
        if not run_script(setup_script):
            print("Setup script failed. Stopping execution.")
            exit(1)  # Exit the script if any setup script fails

    # If setup scripts succeed, proceed to run export scripts in parallel
    print("Running export scripts in parallel...")
    run_export_scripts()