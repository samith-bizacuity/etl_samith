# Imports
import oracledb
import csv
import os
import boto3
from dotenv import load_dotenv
import io

# Load environment variables
load_dotenv()

# Initialize environment variables
oracle_client_path=os.getenv('ORACLE_CLIENT_PATH')
user=os.getenv('ORACLE_USERNAME')
password=os.getenv('ORACLE_PASSWORD')
dsn=os.getenv('ORACLE_DSN')
s3_bucket_name=os.getenv('S3_BUCKET_NAME')
aws_region=os.getenv('AWS_REGION')
access_key=os.getenv('ACCESS_KEY')
secret_key=os.getenv('SECRET_KEY')

# Intialize schema and table names
schema = "CM_20050609"
table = "PRODUCTLINES"

try:
    # Initialize Oracle client
    oracledb.init_oracle_client(lib_dir=oracle_client_path)
except Exception as e:
    print(f"An error occurred during initializing oracle client: {e}")

try:
    # Connect to Oracle DB
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=dsn
    )

    # Create a cursor
    cursor = connection.cursor()

    # Execute the query to fetch all columns and rows from the offices table
    cursor.execute(f"SELECT productLine, textDescription FROM {schema}.{table}")
    
    # Fetch all rows
    rows = cursor.fetchall()
    
    # Get column names
    column_names = [i[0] for i in cursor.description]

    file_name = f'{schema}/{table}/{table}.csv'
    
    # Write data to CSV in memory
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    
    # Write the column names
    csv_writer.writerow(column_names)
    
    # Write the rows
    csv_writer.writerows(rows)
    
    try:
        # Upload CSV to S3
        s3_client = boto3.client('s3', region_name=aws_region,
                                aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key)
        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=file_name,
            Body=csv_buffer.getvalue()
        )

        print(f"Data exported successfully to {file_name} to S3 bucket {s3_bucket_name}")

    # Catch error
    except Exception as e:
        print(f"An error occurred during export of {table} to S3 Bucket: {e}")

# Catch error    
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed")
