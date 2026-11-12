from datetime import datetime, timedelta
import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# ------------------------------------------------------------------
# Define Task Logic Functions
# ------------------------------------------------------------------
def execute_job_1():
    logging.info("Starting Job 1: Initialization tasks...")
    logging.info("Job 1 finished successfully.")

def execute_job_2(should_fail=False):
    logging.info("Starting Job 2: Validation check...")
    if should_fail:
        raise ValueError("Job 2 encountered a critical error! Aborting sequence.")
    logging.info("Job 2 finished successfully.")

def execute_job_3():
    logging.info("Starting Job 3: Finalizing tasks...")
    logging.info("Job 3 finished successfully.")

# ------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------
default_args = {
    "owner": "cde_admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1, tzinfo=pendulum.timezone("UTC")),
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}

dag = DAG(
    dag_id="cde_independent_airflow_jobs",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["cde", "native_airflow", "no_spark"],
)

with dag:
    # Job 1: Executes custom Python function
    job_1 = PythonOperator(
        task_id="job_1_init",
        python_callable=execute_job_1,
    )

    # Job 2: Executes Python function (Set op_kwargs={"should_fail": True} to test failure)
    job_2 = PythonOperator(
        task_id="job_2_validation",
        python_callable=execute_job_2,
        op_kwargs={"should_fail": False},
    )

    # Job 3: Executes a native shell command
    job_3 = BashOperator(
        task_id="job_3_cleanup",
        bash_command='echo "Executing Job 3 on CDE Airflow runner host..."',
    )

    # Job 4: Empty marker task
    job_4 = EmptyOperator(
        task_id="job_4_complete",
    )

    # Strict Dependency Chain
    job_1 >> job_2 >> job_3 >> job_4
