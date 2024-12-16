from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import subprocess
import os
import logging

# Initialize logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dag_start_time = [datetime.now()]

# Gluejob script location
gluejob_script_location = "s3://aws-glue-assets-266735844846-eu-north-1/scripts"

# Region name
region_name = "eu-north-1"

# Logging functions
def log_dag_start():
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dag_start_time[0] = datetime.now()
    logger.info(f"DAG started at {start_time}")
    with open("/mnt/c/Users/samith.hegde/Documents/python/logs/log_dag.txt", "a") as log_file:
        log_file.write(f"DAG started at {start_time}\n")

def log_dag_end(status):
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    duration = datetime.now() - dag_start_time[0]
    logger.info(f"DAG ended at {end_time}, Duration: {duration} Status: {status}")
    with open("/mnt/c/Users/samith.hegde/Documents/python/logs/log_dag.txt", "a") as log_file:
        log_file.write(f"DAG ended at {end_time}, Duration: {duration} Status: {status}\n")

def log_task_start(task_instance, log_file_path):
    task_name = task_instance['task'].task_id
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Task {task_name} started at {start_time}")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"Task {task_name} started at {start_time}\n")

def log_task_status(task_instance, status, log_file_path):
    task_name = task_instance['task'].task_id
    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Task {task_name} ended at {end_time}, Status: {status}")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"Task {task_name} ended at {end_time}, Status: {status}\n")

# Function to run source to s3 scripts
def run_oracle_to_s3(table):
    script_path = os.path.join("/mnt/c/Users/samith.hegde/Documents/python/01_python/oracle_to_s3", f"{table}.py")
    print(f"Running script: {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Script {table} failed with error: {result.stderr}")
    print(result.stdout)

def fetch_batch_details():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/01_python/get_etl_variables.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running get_etl_variables: {result.stderr}")
    print(result.stdout)

# Function to run stage to dw scripts
def run_stage_to_dw(table):
    script_path = os.path.join("/mnt/c/Users/samith.hegde/Documents/python/01_python/stage_to_dw", f"{table}.py")
    print(f"Running script: {script_path}")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Script {table} failed with error: {result.stderr}")
    print(result.stdout)

def batch_log_start():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/01_python/start_batch.py"
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error logging into batch_control_log: {result.stderr}")
    print(result.stdout)

def batch_log_end():
    script_path = "/mnt/c/Users/samith.hegde/Documents/python/01_python/end_batch.py"
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
    start_date=datetime(2024, 12, 16),
    schedule_interval=None,  # Run on demand
    catchup=False,
    on_success_callback=lambda context: log_dag_end("SUCCESS"),  # Log DAG success
    on_failure_callback=lambda context: log_dag_end("FAILED"),  # Log DAG failure
)

# Log DAG start time when it starts
log_dag_start_task = PythonOperator(
    task_id="log_dag_start",
    python_callable=log_dag_start,
    dag=dag,
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
            on_execute_callback=lambda ti: log_task_start(ti, f"/mnt/c/Users/samith.hegde/Documents/python/logs/src_to_s3.txt"),
            on_success_callback=lambda ti: log_task_status(ti, "SUCCESS", f"/mnt/c/Users/samith.hegde/Documents/python/logs/src_to_s3.txt"),
            on_failure_callback=lambda ti: log_task_status(ti, "FAILED", f"/mnt/c/Users/samith.hegde/Documents/python/logs/src_to_s3.txt")
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
            on_execute_callback=lambda ti: log_task_start(ti, f"/mnt/c/Users/samith.hegde/Documents/python/logs/s3_to_stg.txt"),
            on_success_callback=lambda ti: log_task_status(ti, "SUCCESS", f"/mnt/c/Users/samith.hegde/Documents/python/logs/s3_to_stg.txt"),
            on_failure_callback=lambda ti: log_task_status(ti, "FAILED", f"/mnt/c/Users/samith.hegde/Documents/python/logs/s3_to_stg.txt")
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

stg_to_dw = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_stg_to_dw",
        dbt_cloud_conn_id="airflow_db_conn",
        job_id=70471823403461,
        timeout=300,
        on_execute_callback=lambda ti: log_task_start(ti, f"/mnt/c/Users/samith.hegde/Documents/python/logs/stg_to_dw.txt"),
        on_success_callback=lambda ti: log_task_status(ti, "SUCCESS", f"/mnt/c/Users/samith.hegde/Documents/python/logs/stg_to_dw.txt"),
        on_failure_callback=lambda ti: log_task_status(ti, "FAILED", f"/mnt/c/Users/samith.hegde/Documents/python/logs/stg_to_dw.txt"),
)

# Set task dependencies
log_dag_start_task >> fetch_batch_details_task >> batch_log_start_task >> oracle_to_s3 >> s3_to_stg >> stg_to_dw >> batch_log_end_task
