from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import CreateS3BucketOperator, UploadFilesToS3Operator, CheckS3FileCount

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

dag = DAG('raw_datalake_dag',
          default_args=default_args,
          description='Move data from local to S3 bucket',
          schedule_interval='@monthly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

create_raw_data_bucket = CreateS3BucketOperator(
    task_id='Create_S3_bucket',
    bucket_name=s3_bucket_name,
    dag=dag
)

create_script_bucket = CreateS3BucketOperator(
    task_id='Create_script_bucket',
    bucket_name=script_bucket_name,
    dag=dag
)

create_datalake_bucket = CreateS3BucketOperator(
    task_id='Create_datalake_bucket',
    bucket_name=airbnb_datalake_bucket_name,
    dag=dag
)

upload_weather_data = UploadFilesToS3Operator(
    task_id='Upload_weather_data',
    bucket_name=s3_bucket_name,
    path='/opt/bitnami/dataset/weather_data/',
    dag=dag
)

upload_covid_data = UploadFilesToS3Operator(
    task_id='Upload_covid_data',
    bucket_name=s3_bucket_name,
    path='/opt/bitnami/dataset/covid19_data/',
    dag=dag
)

upload_airbnb_data = UploadFilesToS3Operator(
    task_id='Upload_airbnb_data',
    bucket_name=s3_bucket_name,
    path='/opt/bitnami/dataset/airbnb_paris_data/',
    dag=dag
)

check_data_quality = CheckS3FileCount(
    task_id='Check_data_quality',
    bucket_name=s3_bucket_name,
    expected_count=6,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [create_raw_data_bucket, create_script_bucket, create_datalake_bucket]

create_raw_data_bucket >> [upload_weather_data, upload_covid_data, upload_airbnb_data]
create_script_bucket >> end_operator
create_datalake_bucket >> end_operator

[upload_weather_data, upload_covid_data, upload_airbnb_data] >> check_data_quality

check_data_quality >> end_operator