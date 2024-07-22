from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3

def delete_glue_database(**kwargs):
    aws_access_key_id = 'AKIA47CRZDM6RQ4TUJ4C'
    aws_secret_access_key = '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'
    region_name = 'us-east-1'

    client = boto3.client(
        'glue',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name
    )
    
    database_name = kwargs['database_name']
    response = client.delete_database(Name=database_name)
    return response

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG('delete_glue_database_dag',
         default_args=default_args,
         description='A simple DAG to delete a Glue database',
         schedule_interval='@once',
         catchup=False) as dag:

    delete_database_task = PythonOperator(
        task_id='delete_glue_database',
        python_callable=delete_glue_database,
        op_kwargs={'database_name': 'dev'},
    )

    delete_database_task
