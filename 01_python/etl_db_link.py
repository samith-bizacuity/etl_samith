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

oracledb.init_oracle_client(lib_dir=oracle_client_path)
connection = oracledb.connect(user=user, password=password, dsn=dsn)

schema = etl_variables['schema']
schema_pw = etl_variables['schema_pw']

cursor = connection.cursor()

queries = [
    "ALTER SESSION SET CURRENT_SCHEMA=h24samith",
    "DROP PUBLIC DATABASE LINK samith_dblink_classicmodels",
    f"CREATE PUBLIC DATABASE LINK samith_dblink_classicmodels CONNECT TO {schema} identified by {schema_pw} USING 'XE'"
]

for query in queries:
    cursor.execute(query)
    connection.commit()

cursor.close()
connection.close()

print(f"DB link pointing to schema {schema} created successfully.")
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

def execute_queries(cursor, queries):
    for query in queries:
        cursor.execute(query)
        cursor.connection.commit()

def main():
    # Load environment variables
    env_vars = load_environment_variables()
    oracle_client_path = env_vars['oracle_client_path']
    user = env_vars['user']
    password = env_vars['password']
    dsn = env_vars['dsn']

    # Get ETL variables
    schema = etl_variables['schema']
    schema_pw = etl_variables['schema_pw']

    # Initialize Oracle client and connect to the database
    initialize_oracle_client(oracle_client_path)
    connection = connect_to_database(user, password, dsn)

    try:
        cursor = connection.cursor()
        
        # Define queries
        queries = [
            "ALTER SESSION SET CURRENT_SCHEMA=h24samith",
            "DROP PUBLIC DATABASE LINK samith_dblink_classicmodels",
            f"CREATE PUBLIC DATABASE LINK samith_dblink_classicmodels CONNECT TO {schema} identified by {schema_pw} USING 'XE'"
        ]

        # Execute queries
        execute_queries(cursor, queries)

        print(f"DB link pointing to schema {schema} created successfully.")
    finally:
        # Ensure the cursor and connection are closed
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()
