from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, 
                                PostgresOperator, 
                                DataQualityEmptyOperator)
from helpers import SqlQueries

STAGING_TABLE = 'immigration_staging'
FACT_TABLE = 'immigration'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sustainify_fact_dag',
          default_args=default_args,
          description='Load and transform fact data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)

create_fact_table = PostgresOperator(
    task_id='Create_Fact_Table',
    dag=dag,
    sql=SqlQueries.create_fact_table.format(table_name=FACT_TABLE),
    postgres_conn_id='redshift',
)

create_staging_table = PostgresOperator(
    task_id='Create_Staging_Table',
    dag=dag,
    sql=SqlQueries.create_fact_table.format(table_name=STAGING_TABLE),
    postgres_conn_id='redshift',
)

drop_staging_table = PostgresOperator(
    task_id='Drop_Staging_Table',
    dag=dag,
    sql=SqlQueries.drop_fact_table.format(table_name=STAGING_TABLE),
    postgres_conn_id='redshift',
)


staging_to_redshift = StageToRedshiftOperator(
    task_id='Staging_to_Redshift',
    dag=dag,
    table=f'public.{STAGING_TABLE}',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/immigration.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

insert_facts = PostgresOperator(
    task_id='Insert_Facts',
    dag=dag,
    sql=SqlQueries.insert_fact,
    postgres_conn_id='redshift',
)


quality_no_duplicates = DataQualityEmptyOperator(
    task_id='Quality_no_Duplicates',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query="""SELECT cicid, COUNT(*) occurrences FROM {table}
                 GROUP BY cicid HAVING 
                 COUNT(*) > 1;""".format(table=FACT_TABLE)
)

end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


start_operator >> [create_fact_table,
                   create_staging_table]
[create_fact_table, create_staging_table] >> staging_to_redshift
staging_to_redshift >> insert_facts
insert_facts >> drop_staging_table
drop_staging_table >> quality_no_duplicates
quality_no_duplicates >> end_operator