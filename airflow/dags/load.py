import airflow
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

aws_access_key_id = 'AKIA47CRZDM6RQ4TUJ4C'
aws_secret_access_key = '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'

dag = DAG(
  dag_id="download_files",
  schedule_interval="@once",
  start_date=airflow.utils.dates.days_ago(0),
)

def upload_to_s3(local_file_path, s3_bucket, s3_key, aws_access_key_id, aws_secret_access_key):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=s3_bucket, replace=True)

start = DummyOperator(
    task_id="Start",
    dag=dag
)

end = DummyOperator(
    task_id="End",
    dag=dag
)

files = ['songs.csv', 'state_codes.csv']

for file in files:
    download_csv = BashOperator(
        task_id=f'download_{file}',
        bash_command=f'curl -o /opt/airflow/{file} --url https://github.com/huynhdoanho/zingstreamp3/blob/main/{file}',
        dag=dag
    )

    upload_file_task = PythonOperator(
        task_id=f'upload_{file}_to_S3',
        python_callable=upload_to_s3,
        op_kwargs={
            'local_file_path': f'/opt/airflow/{file}',
            's3_bucket': 'zingstreamp3',
            's3_key': f'data/{file}',
            'aws_access_key_id': 'AKIA47CRZDM6RQ4TUJ4C',
            'aws_secret_access_key': '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'
        },
        dag=dag
    )

    remove_local_file = BashOperator(
        task_id=f'remove_{file}_at_local',
        bash_command=f'rm /opt/airflow/{file}',
        dag=dag
    )

    start >> download_csv >> upload_file_task >> remove_local_file >> end
