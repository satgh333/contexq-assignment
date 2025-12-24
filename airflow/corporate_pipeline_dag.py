# jm_requirement
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'corporate_data_pipeline',
    default_args=default_args,
    description='Corporate Data ETL and ML Pipeline',
    schedule_interval='@daily',
    catchup=False
)

# EMR Steps
SPARK_STEPS = [
    {
        'Name': 'Corporate Data Pipeline',
        'ActionOnFailure': 'TERMINATE_JOB_FLOW',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--conf', 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                's3://{{ var.value.s3_bucket }}/code/etl_pipeline.py'
            ]
        }
    }
]

# Add EMR step
add_steps = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    aws_conn_id='aws_default',
    steps=SPARK_STEPS,
    dag=dag
)

# Wait for step completion
step_sensor = EmrStepSensor(
    task_id='watch_step',
    job_flow_id='{{ var.value.emr_cluster_id }}',
    step_id='{{ task_instance.xcom_pull(task_ids="add_steps", key="return_value")[0] }}',
    aws_conn_id='aws_default',
    dag=dag
)

add_steps >> step_sensor
