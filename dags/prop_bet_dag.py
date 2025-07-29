from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='prop_bet_pipeline',
    default_args=default_args,
    description='Run Spark job to evaluate prop bets',
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_spark_job = BashOperator(
        task_id='run_spark_prop_bet_job',
        bash_command='docker exec spark-master spark-submit --jars /app/postgresql-42.7.3.jar /app/prop_bet_job.py'
    )
