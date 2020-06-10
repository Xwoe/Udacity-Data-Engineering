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

dag = DAG('test2_sustainify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)



quality_transport = DataQualityOperator(
    task_id='Quality_not_empty_Transport',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.transport',
    check_not_empty=True    
)

quality_dates = DataQualityOperator(
    task_id='Quality_not_empty_Dates',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.dates',
    check_not_empty=True    
)

quality_country = DataQualityOperator(
    task_id='Quality_not_empty_Country',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.country',
    check_not_empty=True    
)

quality_temperature_annual_country = DataQualityOperator(
    task_id='Quality_not_empty_Temperature_Annual_Country',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.temperature_annual_country',
    check_not_empty=True    
)

quality_city_demographics = DataQualityOperator(
    task_id='Quality_not_empty_City_Demographics',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.city_demographics',
    check_not_empty=True    
)

quality_visa = DataQualityOperator(
    task_id='Quality_not_empty_Visa',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.visa',
    check_not_empty=True
)

quality_temperature_global = DataQualityOperator(
    task_id='Quality_not_empty_Temperature_Global',
    dag=dag,
    redshift_conn_id='redshift',
    sql_query='SELECT COUNT(*) FROM public.temperature_global',
    check_not_empty=True
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





end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


start_operator >> [quality_transport,
                  quality_dates,
                  quality_country,
                  quality_temperature_annual_country,
                  quality_city_demographics,
                  quality_visa,
                  quality_temperature_global
                  ]

[quality_transport,
 quality_dates,
 quality_country,
 quality_temperature_annual_country,
 quality_city_demographics,
 quality_visa,
 quality_temperature_global
] >> end_operator
