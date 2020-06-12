from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                                DataQualityOperator,
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

dag = DAG('sustainify_dimensions_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

start_operator = DummyOperator(task_id='Begin_Execution',  dag=dag)

create_sql_tables = PostgresOperator(
    task_id='Create_Tables',
    dag=dag,
    sql=SqlQueries.create_tables,
    postgres_conn_id='redshift',
)

transport_to_redshift = StageToRedshiftOperator(
    task_id='Transport_to_Redshift',
    dag=dag,
    table='public.transport',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/transport.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

country_to_redshift = StageToRedshiftOperator(
    task_id='Country_to_Redshift',
    dag=dag,
    table='public.country',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/country.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

temperature_annual_country_to_redshift = StageToRedshiftOperator(
    task_id='Temperature_Annual_Country_to_Redshift',
    dag=dag,
    table='public.temperature_annual_country',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/temperature_annual_country.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

temperature_global_to_redshift = StageToRedshiftOperator(
    task_id='Temperature_Global_to_Redshift',
    dag=dag,
    table='public.temperature_global',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/temperature_global.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

visa_to_redshift = StageToRedshiftOperator(
    task_id='Visa_to_Redshift',
    dag=dag,
    table='public.visa',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/visa.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

city_demographics_to_redshift = StageToRedshiftOperator(
    task_id='City_Demographics_to_Redshift',
    dag=dag,
    table='public.city_demographics',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/city_demographics.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

dates_to_redshift = StageToRedshiftOperator(
    task_id='Dates_to_Redshift',
    dag=dag,
    table='public.dates',
    redshift_conn_id='redshift',
    s3_bucket='xwoe-udacity',
    s3_key='deng_capstone/tables/dates.parquet',
    aws_credentials_id='aws_credentials',
    aws_iam=Variable.get('arn'),
    file_format="FORMAT AS PARQUET"
)

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


end_operator = DummyOperator(task_id='Stop_Execution',  dag=dag)


start_operator >> create_sql_tables

create_sql_tables >> [transport_to_redshift,
                      country_to_redshift,
                      temperature_annual_country_to_redshift,
                      temperature_global_to_redshift,
                      visa_to_redshift,
                      city_demographics_to_redshift,
                      dates_to_redshift
                     ]

transport_to_redshift >> quality_transport
country_to_redshift >> quality_country
temperature_annual_country_to_redshift >> quality_temperature_annual_country
temperature_global_to_redshift >> quality_temperature_global
visa_to_redshift >> quality_visa
city_demographics_to_redshift >> quality_city_demographics
dates_to_redshift >> quality_dates


[quality_transport,
                  quality_dates,
                  quality_country,
                  quality_temperature_annual_country,
                  quality_city_demographics,
                  quality_visa,
                  quality_temperature_global
                  ] >> end_operator