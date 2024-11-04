import redshift_connector
from dotenv import load_dotenv
import os
import json

load_dotenv()

redshift_host = os.getenv('REDSHIFT_HOST')
redshift_database = os.getenv('REDSHIFT_DATABASE')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')

conn = redshift_connector.connect(
    host=redshift_host,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password,
)

try:
    cursor = conn.cursor()
except Exception as e:
    print(f"An error occurred in connecting to AWS Redshift: {e}")

try:
    cursor.execute("SELECT etl_batch_no, to_char(etl_batch_date, 'yyyy-mm-dd') FROM etl_metadata.batch_control")
except Exception as e:
    print(f"An error occurred while retrieving etl_batch_no and etl_batch_date: {e}")

etl_variables = cursor.fetchone()
etl_batch_no = etl_variables[0]
etl_batch_date = etl_variables[1]

cursor.close()
conn.close()

print(f"ETL Batch No: {etl_batch_no} \n ETL batch date: {etl_batch_date}")

with open('etl_variables.json', 'w') as f:
    json.dump({'etl_batch_no': etl_batch_no, 'etl_batch_date': etl_batch_date}, f)
    print("ETL variables written to etl_variables.py")

