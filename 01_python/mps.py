import os
import subprocess
from dotenv import load_dotenv
import json
import redshift_connector
from tables import tables
from queries import queries

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def load_environment_variables():
    load_dotenv()
    return {
        'aws_region': os.getenv('AWS_REGION'),
        'redshift_host': os.getenv('REDSHIFT_HOST'),
        'redshift_database': os.getenv('REDSHIFT_DATABASE'),
        'redshift_user': os.getenv('REDSHIFT_USER'),
        'redshift_password': os.getenv('REDSHIFT_PASSWORD'),
    }

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
    
def main():
    env_vars = load_environment_variables()
    table = 'MONTHLY_PRODUCT_SUMMARY'
    query_update = queries[table][0]
    query_insert = queries[table][1]

    redshift_conn = connect_to_redshift(env_vars['redshift_host'], env_vars['redshift_database'], env_vars['redshift_user'], env_vars['redshift_password'])

    # Fetch ETL variables
    subprocess.run(["python", "get_etl_variables.py"], capture_output=True, text=True, check=True)

    with open('etl_variables.json', 'r') as f:
        etl_variables = json.load(f)
        etl_batch_no = etl_variables['etl_batch_no']
        etl_batch_date = etl_variables['etl_batch_date']

    try:
        cursor = redshift_conn.cursor()
        cursor.execute(query_update, (etl_batch_date, etl_batch_no, etl_batch_date))
        redshift_conn.commit()
        cursor.execute(query_insert, (etl_batch_no, etl_batch_date))
        redshift_conn.commit()
        print(f"Data successfully from devstage to devdw for {table} in Redshift")
    except Exception as e:
        print(f"An error occurred during loading data from devstage to devdw: {e}")
    finally:
        cursor.close()

    redshift_conn.close()

if __name__ == "__main__":
    main()
