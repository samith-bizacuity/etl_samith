# Imports
import os
from dotenv import load_dotenv
import redshift_connector
import json

# Set current directory as the working directory
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
def load_environment_variables():
    load_dotenv()
    return {
        'aws_region': os.getenv('AWS_REGION'),
        'redshift_host': os.getenv('REDSHIFT_HOST'),
        'redshift_database': os.getenv('REDSHIFT_DATABASE'),
        'redshift_user': os.getenv('REDSHIFT_USER'),
        'redshift_password': os.getenv('REDSHIFT_PASSWORD'),
    }

# Connect to Redshift
def connect_to_redshift(host, database, user, password):
    try:
        return redshift_connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
    except Exception as e:
        print(f"An error occurred while connecting to Redshift: {e}")
        return None
    
# Execute the end batch query
def execute_end_batch(redshift_conn, etl_batch_no):
    query = f"""
            UPDATE etl_metadata.batch_control_log
            SET etl_batch_status = 'C', etl_batch_end_time = CURRENT_TIMESTAMP
            WHERE etl_batch_no = {etl_batch_no};
            """
    
    cursor = redshift_conn.cursor()

    try:
        cursor.execute(query)
        redshift_conn.commit()
        print(f"Batch Control Log updated successfully for ETL Batch No: {etl_batch_no}")
    except Exception as e:
        print(f"An error occurred while executing the end batch query: {e}")
    finally:
        cursor.close()

# Main function
def main():
    env_vars = load_environment_variables()

    # Load variables given as arguments to the script for execution
    with open('/mnt/c/Users/samith.hegde/Documents/python/02_glue/etl_variables.json', 'r') as f:
        etl_variables = json.load(f)

    etl_batch_no = etl_variables['etl_batch_no']

    redshift_conn = connect_to_redshift(env_vars['redshift_host'], env_vars['redshift_database'], env_vars['redshift_user'], env_vars['redshift_password'])

    try:
        execute_end_batch(redshift_conn, etl_batch_no)
    except Exception as e:
        print(f"An error occurred while updating the Batch Control Log table: {e}")
    finally:
        redshift_conn.close()

if __name__ == "__main__":
    main()