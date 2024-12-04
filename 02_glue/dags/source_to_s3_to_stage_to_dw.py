from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import subprocess
import os
import json

# Gluejob script location
gluejob_script_location = "s3://aws-glue-assets-266735844846-eu-north-1/scripts"

# Region name
region_name = "eu-north-1"

# Function to run source to s3 scripts
def run_oracle_to_s3(table):
    script_path = os.path.join("/mnt/c/Users/samith.hegde/Documents/python/02_glue/oracle_to_s3", f"{table}.py")
    print(f"Running script: {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Script {table} failed with error: {result.stderr}")
    print(result.stdout)

def fetch_batch_details():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/02_glue/get_etl_variables.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running get_etl_variables: {result.stderr}")
    print(result.stdout)

# Function to run stage to dw scripts
def run_stage_to_dw(table):
    script_path = os.path.join("/mnt/c/Users/samith.hegde/Documents/python/02_glue/stage_to_dw", f"{table}.py")
    print(f"Running script: {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Script {table} failed with error: {result.stderr}")
    print(result.stdout)

def batch_log_start():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/02_glue/start_batch.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error logging into batch_control_log: {result.stderr}")
    print(result.stdout)

def batch_log_end():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/02_glue/end_batch.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error logging into batch_control_log: {result.stderr}")
    print(result.stdout)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    dag_id="source_to_s3_to_stage_to_dw",
    default_args=default_args,
    start_date=datetime(2024, 12, 4),
    schedule_interval=None,  # Run on demand
    catchup=False,
)

# Fetch batch details
fetch_batch_details_task = PythonOperator(
    task_id="fetch_batch_details",
    python_callable=fetch_batch_details,
    dag=dag,
)

batch_log_start_task = PythonOperator(
    task_id="batch_log_start",
    python_callable=batch_log_start,
    dag=dag,
)

batch_log_end_task = PythonOperator(
    task_id="batch_log_end",
    python_callable=batch_log_end,
    dag=dag,
)

tables = ["customers",
    "employees",
    "offices",
    "orderdetails",
    "orders",
    "payments",
    "productlines",
    "products"]

# Define Task Group for oracle_to_s3
with TaskGroup("oracle_to_s3", dag=dag) as oracle_to_s3:
    oracle_to_s3_tasks = [
        PythonOperator(
            task_id=f"src_to_s3_{table}",
            python_callable=run_oracle_to_s3,
            op_args=[table],
            dag=dag,
        )
        for table in tables
    ]

# Define Task Group for s3_to_stg
with TaskGroup("s3_to_stg", dag=dag) as s3_to_stg:
    s3_to_stg_tasks=[
        GlueJobOperator(
            task_id=f"s3_to_stg_gluejob_{job_name}",
            job_name=job_name,
            script_location=f"{gluejob_script_location}/{job_name}.py",
            region_name=region_name,
            aws_conn_id="aws_default",
            dag=dag,
        )
        for job_name in tables
    ]

    # Add a success aggregator to ensure all Glue jobs are complete
    glue_aggregator = DummyOperator(
        task_id="glue_aggregator",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Ensure all Glue jobs succeed
        dag=dag,
    )

    s3_to_stg_tasks >> glue_aggregator  # All Glue tasks flow into the aggregator

dwTables = ["offices",
    "employees",
    "customers",
    "customer_history",
    "payments",
    "orders",
    "productlines",
    "products",
    "product_history",
    "orderdetails",
    "dcs",
    "dps",
    "mcs",
    "mps",]

with TaskGroup("stg_to_dw", dag=dag) as stg_to_dw:
    stg_to_dw_tasks = [
        PythonOperator(
            task_id=f"stg_to_dw_{table}",
            python_callable=run_stage_to_dw,
            op_args=[table],
            dag=dag,
        )
        for table in dwTables
    ]

    temp = DummyOperator(
    task_id="temp",
    dag=dag,
    )

stg_to_dw_tasks[0] >> stg_to_dw_tasks[1] >> stg_to_dw_tasks[2] >> [stg_to_dw_tasks[3], stg_to_dw_tasks[4], stg_to_dw_tasks[5]] >> stg_to_dw_tasks[6] >> stg_to_dw_tasks[7] >> stg_to_dw_tasks[8] >> stg_to_dw_tasks[9] >> [stg_to_dw_tasks[10], stg_to_dw_tasks[11]] >> temp >> [stg_to_dw_tasks[12], stg_to_dw_tasks[13]]

# Set task dependencies
fetch_batch_details_task >> batch_log_start_task >> oracle_to_s3 >> s3_to_stg >> stg_to_dw >> batch_log_end_task
