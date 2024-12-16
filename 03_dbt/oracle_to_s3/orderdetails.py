# Imports
import oracledb
import csv
import os
import boto3
from dotenv import load_dotenv
import io
import sys
from datetime import datetime
from tables import tables
import json

def load_environment_variables():
    load_dotenv()
    return {
        'oracle_client_path': os.getenv('ORACLE_CLIENT_PATH'),
        'oracle_client_path_linux': os.getenv('ORACLE_CLIENT_PATH_LINUX'),
        'user': os.getenv('ORACLE_USERNAME'),
        'password': os.getenv('ORACLE_PASSWORD'),
        'dsn': os.getenv('ORACLE_DSN'),
        's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
        'aws_region': os.getenv('AWS_REGION')
    }

def initialize_oracle_client(client_path):
    try:
        oracledb.init_oracle_client(lib_dir=client_path)
    except Exception as e:
        print(f"An error occurred during initializing oracle client: {e}")

def connect_to_oracle(user, password, dsn):
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as e:
        print(f"An error occurred while connecting to Oracle DB: {e}")
        return None

def fetch_data(cursor, schema, table, etl_batch_date):
    query = f"SELECT {', '.join(tables[table])} FROM {table}@{schema} WHERE UPDATE_TIMESTAMP >= date '{etl_batch_date}'"
    try:
        cursor.execute(query)
    except Exception as e:
        print(f"An error occured: {e}")
    rows = cursor.fetchall()
    columns = [i[0] for i in cursor.description]
    return rows, columns

def write_data_to_csv(rows, column_names):
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(column_names)
    csv_writer.writerows(rows)
    return csv_buffer.getvalue()

def upload_to_s3(s3_client, bucket_name, file_name, data):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=data)
        print(f"Data exported successfully to {file_name} in S3 bucket {bucket_name}")
    except Exception as e:
        print(f"An error occurred during export of data to S3 Bucket: {e}")

def main():
    env_vars = load_environment_variables()
    schema = "samith_dblink_classicmodels"
    table = "ORDERDETAILS"

    if sys.platform == 'linux':
        initialize_oracle_client(env_vars['oracle_client_path_linux'])
    else:
        initialize_oracle_client(env_vars['oracle_client_path'])
    
    connection = connect_to_oracle(env_vars['user'], env_vars['password'], env_vars['dsn'])
    if not connection:
        return

    cursor = connection.cursor()
    try:
        with open('/mnt/c/Users/samith.hegde/Documents/python/02_glue/etl_variables.json', 'r') as f:
            etl_variables = json.load(f)

        etl_batch_date = etl_variables['etl_batch_date']
        etl_batch_date = datetime.strptime(etl_batch_date, "%Y-%m-%d").date()
        rows, column_names = fetch_data(cursor, schema, table, etl_batch_date)

        file_name = f'{table}/{etl_batch_date}/{table}.csv'
        csv_data = write_data_to_csv(rows, column_names)

        s3_client = boto3.client('s3', region_name=env_vars['aws_region'])
        upload_to_s3(s3_client, env_vars['s3_bucket_name'], file_name, csv_data)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        cursor.close()
        connection.close()
        print("Connection closed")

if __name__ == "__main__":
    main()
