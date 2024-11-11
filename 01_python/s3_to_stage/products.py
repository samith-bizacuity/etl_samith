import os
from dotenv import load_dotenv
import sys
import redshift_connector
from tables import tables

os.chdir(os.path.dirname(os.path.abspath(__file__)))

def load_environment_variables():
    load_dotenv()
    return {
        'aws_region': os.getenv('AWS_REGION'),
        's3_bucket_name': os.getenv('S3_BUCKET_NAME'),
        'redshift_host': os.getenv('REDSHIFT_HOST'),
        'redshift_database': os.getenv('REDSHIFT_DATABASE'),
        'redshift_user': os.getenv('REDSHIFT_USER'),
        'redshift_password': os.getenv('REDSHIFT_PASSWORD'),
        'iam_role': os.getenv('REDSHIFT_IAMROLE')
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
    table = 'PRODUCTS'

    redshift_conn = connect_to_redshift(env_vars['redshift_host'], env_vars['redshift_database'], env_vars['redshift_user'], env_vars['redshift_password'])
    s3_bucket = env_vars['s3_bucket_name']
    iam_role = env_vars['iam_role']

    etl_batch_date = sys.argv[1]

    try:
        cursor = redshift_conn.cursor()
        cursor.execute(f"TRUNCATE TABLE devstage.{table}")
        cursor.execute(f"COPY devstage.{table} ({', '.join(tables[table])}) FROM 's3://{s3_bucket}/{table}/{etl_batch_date}/{table}.csv' IAM_ROLE '{iam_role}' FORMAT AS CSV DELIMITER ',' QUOTE '\"' IGNOREHEADER 1 REGION AS 'eu-north-1'")
        redshift_conn.commit()
        print(f"Data loaded successfully to {table} in Redshift")
    except Exception as e:
        print(f"An error occurred during loading data to Redshift: {e}")
    finally:
        cursor.close()

    redshift_conn.close()

if __name__ == "__main__":
    main()
