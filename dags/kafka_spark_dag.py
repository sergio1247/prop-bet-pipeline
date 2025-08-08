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
    dag_id='kafka_spark_pipeline',
    default_args=default_args,
    description='Daily prop bet analysis at midnight',
    schedule_interval='0 0 * * *',  # Run at midnight every day
    catchup=False
) as dag:

    # Send message to Kafka topic via REST API
    send_kafka_message_task = BashOperator(
        task_id='send_spark_trigger',
        bash_command='''TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ); curl -X POST http://kafka-rest:8082/topics/spark-jobs -H "Content-Type: application/vnd.kafka.json.v2+json" -d '{"records":[{"value":{"job_type": "prop_bet_analysis", "timestamp": "'$TIMESTAMP'"}}]}' '''
    )

    # Just send Kafka message - Spark will process it automatically when it runs
    # Note: Run Spark job manually or set up a separate process to monitor Kafka
    log_message = BashOperator(
        task_id='log_completion',
        bash_command='echo "Kafka message sent successfully. Run Spark job manually: docker exec spark-master /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --jars /app/postgresql-42.7.3.jar /app/prop_bet_job.py"'
    )
    
    # Chain the tasks
    send_kafka_message_task >> log_message