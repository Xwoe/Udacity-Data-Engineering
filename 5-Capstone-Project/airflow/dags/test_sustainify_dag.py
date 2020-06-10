from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator,
                                PostgresOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('test_sustainify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)

create_sql_tables = PostgresOperator(
    task_id='Create_Tables',
    dag=dag,
    sql=SqlQueries.create_tables,
    postgres_conn_id='redshift',
)

# TODO for debug purposes
drop_sql_tables = PostgresOperator(
    task_id='Drop_Tables',
    dag=dag,
    sql=SqlQueries.drop_tables,
    postgres_conn_id='redshift',
)


temp_ann_to_redshift = StageToRedshiftOperator(
    task_id='temperature_annual_country',
    dag=dag,
    table='public.temperature_annual_country',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/temperature_annual_country.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)



end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


start_operator >> drop_sql_tables
drop_sql_tables >> create_sql_tables

create_sql_tables >> temp_ann_to_redshift
temp_ann_to_redshift >> end_operator
