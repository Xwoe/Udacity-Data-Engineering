from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
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

dag = DAG('sustainify_dag',
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


stage_model_to_redshift = StageToRedshiftOperator(
    task_id='Stage_Model',
    dag=dag,
    table='public.transport',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/transport.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn', default_var='arn:aws:iam::500028201692:role/myRedshiftRole'),
    #region='us-east-2',
    file_format="FORMAT AS PARQUET"
    #file_format="""FORMAT AS CSV DELIMITER ',' QUOTE '"' 
    #               IGNOREHEADER 1"""
    
)


"""
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    select_sql=SqlQueries.songplay_table_insert,
    table='public.songplays'
)
"""

"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.users WHERE userid = NULL',
    expected_result=0
    
)
"""

end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


start_operator >> drop_sql_tables
drop_sql_tables >> create_sql_tables
#create_sql_tables >> time_spark

create_sql_tables >> stage_model_to_redshift

#time_spark >> end_operator
stage_model_to_redshift >> end_operator