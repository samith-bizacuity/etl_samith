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