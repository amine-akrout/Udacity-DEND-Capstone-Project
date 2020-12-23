import airflow
import configparser
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from operators import CreateS3BucketOperator, UploadFilesToS3Operator


s3_bucket_name = 'dend-capstone-aak-raw-data'
script_bucket_name = 'dend-capstone-aak-spark-script'
airbnb_datalake_bucket_name = 'dend-capstone-aak-airbnb-datalake'

default_args = {
    'owner': 'aamine',
    'start_date': datetime.now(),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
    'email_on_retry': False
}

SPARK_ETL_STEPS = [
    {
        'Name': 'Setup Debugging',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['state-pusher-script']
        }
    },
    {
        'Name': 'Setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://' + script_bucket_name, '/home/hadoop/', '--recursive']
        }
    },
    {
        'Name': 'Weather - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/weather_etl.py', 's3a://' + s3_bucket_name,
                     's3a://' + airbnb_datalake_bucket_name]
        }
    },
    {
        'Name': 'Covid19 - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/covid_etl.py', 's3a://' + s3_bucket_name,
                     's3a://' + airbnb_datalake_bucket_name]
        }
    },
    {
        'Name': 'Airbnb - ETL',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/airbnb_etl.py', 's3a://' + s3_bucket_name,
                     's3a://' + airbnb_datalake_bucket_name]
        }
    },
    {
        'Name': 'Check data quality',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/script/check_data_quality.py',
                     's3a://' + airbnb_datalake_bucket_name]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'Airbnb-Datalake-ETL'
}

dag = DAG('airbnb_data_etl_dag',
          default_args=default_args,
          description='Extract transform and load data to S3 datalake.',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


upload_etl_code = UploadFilesToS3Operator(
    task_id='Upload_etl_code',
    bucket_name=script_bucket_name,
    path='/opt/bitnami/script/',
    dag=dag
)


create_cluster = EmrCreateJobFlowOperator(
    task_id='Create_EMR_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    region_name='eu-west-3',
    dag=dag
)

add_jobflow_steps = EmrAddStepsOperator(
    task_id='Add_jobflow_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='Create_EMR_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    region_name='eu-west-3',
    steps=SPARK_ETL_STEPS,
    dag=dag
)

weather_processing = EmrStepSensor(
    task_id='weather_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[2] }}",
    aws_conn_id='aws_credentials',
    region_name='eu-west-3',
    dag=dag
    )

covid_processing = EmrStepSensor(
    task_id='covid_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[3] }}",
    aws_conn_id='aws_credentials',
    region_name='eu-west-3',
    dag=dag
    )

airbnb_processing = EmrStepSensor(
    task_id='airbnb_processing_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[4] }}",
    aws_conn_id='aws_credentials',
    region_name='eu-west-3',
    dag=dag
    )

data_quality_check = EmrStepSensor(
    task_id='data_quality_check_step',
    job_flow_id="{{ task_instance.xcom_pull('Create_EMR_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='Add_jobflow_steps', key='return_value')[5] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Define dependency
start_operator >> upload_etl_code
upload_etl_code >> create_cluster
create_cluster >> add_jobflow_steps
add_jobflow_steps >> [weather_processing, covid_processing, airbnb_processing]
[weather_processing, covid_processing, airbnb_processing] >> data_quality_check
data_quality_check >> end_operator