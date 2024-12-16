import sys
import redshift_connector
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ['aws_region', 's3_bucket_name', 'redshift_host', 'redshift_database', 'redshift_user', 'redshift_password', 'iam_role'])

def load_environment_variables():
    return {
        'aws_region': args['aws_region'],
        's3_bucket_name': args['s3_bucket_name'],
        'redshift_host': args['redshift_host'],
        'redshift_database': args['redshift_database'],
        'redshift_user': args['redshift_user'],
        'redshift_password': args['redshift_password'],
        'iam_role': args['iam_role']
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
    table = 'ORDERS'

    redshift_conn = connect_to_redshift(env_vars['redshift_host'], env_vars['redshift_database'], env_vars['redshift_user'], env_vars['redshift_password'])
    s3_bucket = env_vars['s3_bucket_name']
    iam_role = env_vars['iam_role']

    try:
        cursor = redshift_conn.cursor()
        cursor.execute("SELECT etl_batch_no, to_char(etl_batch_date, 'yyyy-mm-dd') FROM etl_metadata.batch_control")
        etl_variables = cursor.fetchone()
        etl_batch_no = etl_variables[0]
        etl_batch_date = etl_variables[1]
    except Exception as e:
        print(f"An error occurred during fetching etl batch variables: {e}")
    finally:
        cursor.close()
        
    try:
        cursor = redshift_conn.cursor()
        cursor.execute(f"TRUNCATE TABLE devstage.{table}")
        cursor.execute(f"COPY devstage.{table} FROM 's3://{s3_bucket}/{table}/{etl_batch_date}/{table}.csv' IAM_ROLE '{iam_role}' FORMAT AS CSV DELIMITER ',' QUOTE '\"' IGNOREHEADER 1 REGION AS 'eu-north-1'")
        redshift_conn.commit()
        print(f"Data loaded successfully to {table} in Redshift")
    except Exception as e:
        print(f"An error occurred during loading data to Redshift: {e}")
    finally:
        cursor.close()

    redshift_conn.close()

if __name__ == "__main__":
    main()