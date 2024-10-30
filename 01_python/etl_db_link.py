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