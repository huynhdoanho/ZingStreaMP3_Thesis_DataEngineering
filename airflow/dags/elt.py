from datetime import date, timedelta
import airflow
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.operators.python_operator import PythonOperator
import boto3


# declare time
today = date.today()
yesterday = today - timedelta(days = 1)
month = 7#yesterday.month
day = 11#yesterday.day


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


# create DAG
dag = DAG(
  dag_id="ZingStreaMP3",
  schedule_interval="0 1 * * *",    # 00:01:00
  start_date=airflow.utils.dates.days_ago(0),
)


# tasks
start = DummyOperator(
    task_id="Start",
    dag=dag
)

end = DummyOperator(
    task_id="End",
    dag=dag
)

create_external_schema = PostgresOperator(
    task_id='create_external_schema',
    postgres_conn_id='redshift_zingstreamp3',  # Kết nối Redshift đã được cấu hình trong Airflow
    sql='./sql/create_external_schema.sql',
    dag=dag
)

create_external_table_auth_events = RedshiftSQLOperator(
    task_id='create_external_table_auth_events',
    redshift_conn_id="redshift_zingstreamp3",
    sql=f"""
        CREATE EXTERNAL TABLE spectrum_schema.auth_events (
            city VARCHAR(200),
            firstName VARCHAR(200),
            gender VARCHAR(200),
            itemInSession INT,
            lastName VARCHAR(200),
            lat DOUBLE PRECISION,
            level VARCHAR(200),
            lon DOUBLE PRECISION,
            registration BIGINT,
            sessionId INT,
            state VARCHAR(200),
            success BOOLEAN,
            ts TIMESTAMP,
            userAgent VARCHAR(200),
            userId BIGINT,
            zip VARCHAR(200),
            year INT,
            month INT,
            day INT,
            hour INT
        )
        STORED AS PARQUET
        LOCATION 's3://zingstreamp3/testing/auth_events/month={month}/day={day}';
        """,
    dag=dag
)

create_external_table_listen_events = RedshiftSQLOperator(
    task_id='create_external_table_listen_events',
    redshift_conn_id="redshift_zingstreamp3",
    sql=f"""
        CREATE EXTERNAL TABLE spectrum_schema.listen_events (
            artist VARCHAR(200),
            song VARCHAR(200),
            duration DOUBLE PRECISION,
            ts TIMESTAMP,
            sessionId INT,
            auth VARCHAR(200),
            level VARCHAR(200),
            itemInSession INT,
            city VARCHAR(200),
            zip VARCHAR(200),
            state VARCHAR(200),
            userAgent VARCHAR(200),
            lon DOUBLE PRECISION,
            lat DOUBLE PRECISION,
            userId BIGINT,
            lastName VARCHAR(200),
            firstName VARCHAR(200),
            gender VARCHAR(200),
            registration BIGINT,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        STORED AS PARQUET
        LOCATION 's3://zingstreamp3/testing/listen_events/month={month}/day={day}';
    """,
    dag=dag
)

create_external_table_page_view_events = RedshiftSQLOperator(
    task_id='create_external_table_page_view_events',
    redshift_conn_id="redshift_zingstreamp3",
    sql=f"""
        CREATE EXTERNAL TABLE spectrum_schema.page_view_events (
            artist VARCHAR(200),
            auth VARCHAR(200),
            city VARCHAR(200),
            duration DOUBLE PRECISION,
            firstName VARCHAR(200),
            gender VARCHAR(200),
            itemInSession INT,
            lastName VARCHAR(200),
            lat DOUBLE PRECISION,
            level VARCHAR(200),
            lon DOUBLE PRECISION,
            method VARCHAR(200),
            page VARCHAR(200),
            registration BIGINT,
            sessionId INT,
            song VARCHAR(200),
            state VARCHAR(200),
            status INT,
            success BOOLEAN,
            ts TIMESTAMP,
            userAgent VARCHAR(200),
            userId BIGINT,
            zip VARCHAR(200),
            year INT,
            month INT,
            day INT,
            hour INT
        )
        STORED AS PARQUET
        LOCATION 's3://zingstreamp3/testing/page_view_events/month={month}/day={day}';
    """,
    dag=dag
)

create_external_table_status_change_events = RedshiftSQLOperator(
    task_id='create_external_table_status_change_events',
    redshift_conn_id="redshift_zingstreamp3",
    sql=f"""
        CREATE EXTERNAL TABLE spectrum_schema.status_change_events (
            artist VARCHAR(200),
            auth VARCHAR(200),
            city VARCHAR(200),
            duration DOUBLE PRECISION,
            firstName VARCHAR(200),
            gender VARCHAR(200),
            itemInSession INT,
            lastName VARCHAR(200),
            lat DOUBLE PRECISION,
            level VARCHAR(200),
            lon DOUBLE PRECISION,
            method VARCHAR(200),
            page VARCHAR(200),
            registration BIGINT,
            sessionId INT,
            song VARCHAR(200),
            state VARCHAR(200),
            status INT,
            success BOOLEAN,
            ts TIMESTAMP,
            userAgent VARCHAR(200),
            userId BIGINT,
            zip VARCHAR(200),
            year INT,
            month INT,
            day INT,
            hour INT
        )
        STORED AS PARQUET
        LOCATION 's3://zingstreamp3/testing/status_change_events/month={month}/day={day}';
    """,
    dag=dag
)

dbt_task = BashOperator(
    task_id = 'dbt_run',
    bash_command='cd /opt/airflow/mydbt && dbt deps && dbt run --profiles-dir . ',
    dag = dag
)
# && dbt seed --target staging 

delete_glue_task = PythonOperator(
    task_id='delete_glue_database',
    python_callable=delete_glue_database,
    op_kwargs={'database_name': 'dev'},
    dag=dag  
)

drop_schema = RedshiftSQLOperator(
    task_id='drop_spectrum_schema',
    redshift_conn_id="redshift_zingstreamp3",
    sql=f"""
        DROP SCHEMA spectrum_schema
    """,
    dag=dag
)


# run
start >> \
create_external_schema >> \
[create_external_table_auth_events, create_external_table_listen_events, create_external_table_page_view_events, create_external_table_status_change_events] >> \
dbt_task >> \
delete_glue_task >> \
drop_schema >> \
end
