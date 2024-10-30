"""
import os
import oracledb
from dotenv import load_dotenv
from etl_variables import etl_variables

load_dotenv()
oracle_client_path = os.getenv('ORACLE_CLIENT_PATH')
user = os.getenv('ORACLE_USERNAME')
password = os.getenv('ORACLE_PASSWORD')
dsn = os.getenv('ORACLE_DSN')

etl_batch_no = etl_variables['etl_batch_no']
etl_batch_date = etl_variables['etl_batch_date']

oracledb.init_oracle_client(lib_dir=oracle_client_path)
connection = oracledb.connect(user=user, password=password, dsn=dsn)

query = f"UPDATE h24samith.etl_batch_control SET etl_batch_no = {etl_batch_no}, etl_batch_date = date '{etl_batch_date}'"

cursor = connection.cursor()
cursor.execute(query)
connection.commit() 
cursor.close()

connection.close()

print("ETL batch control updated successfully.")
"""

import os
import oracledb
from dotenv import load_dotenv
from etl_variables import etl_variables

def load_environment_variables():
    load_dotenv()
    return {
        'oracle_client_path': os.getenv('ORACLE_CLIENT_PATH'),
        'user': os.getenv('ORACLE_USERNAME'),
        'password': os.getenv('ORACLE_PASSWORD'),
        'dsn': os.getenv('ORACLE_DSN')
    }

def initialize_oracle_client(client_path):
    oracledb.init_oracle_client(lib_dir=client_path)

def connect_to_database(user, password, dsn):
    return oracledb.connect(user=user, password=password, dsn=dsn)

def update_etl_batch_control(connection, etl_batch_no, etl_batch_date):
    query = f"""
        UPDATE h24samith.etl_batch_control
        SET etl_batch_no = {etl_batch_no},
            etl_batch_date = date '{etl_batch_date}'
    """
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()

def main():
    # Load environment variables
    env_vars = load_environment_variables()
    oracle_client_path = env_vars['oracle_client_path']
    user = env_vars['user']
    password = env_vars['password']
    dsn = env_vars['dsn']

    # Get ETL variables
    etl_batch_no = etl_variables['etl_batch_no']
    etl_batch_date = etl_variables['etl_batch_date']

    # Initialize Oracle client and connect to the database
    initialize_oracle_client(oracle_client_path)
    connection = connect_to_database(user, password, dsn)

    try:
        # Update ETL batch control
        update_etl_batch_control(connection, etl_batch_no, etl_batch_date)
        print("ETL batch control updated successfully.")
    finally:
        # Ensure the connection is closed
        connection.close()

if __name__ == "__main__":
    main()
