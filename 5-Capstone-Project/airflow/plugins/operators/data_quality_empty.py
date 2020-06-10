from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityEmptyOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 *args, **kwargs):

        super(DataQualityEmptyOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        
    def execute(self, context):
        
        self.log.info('Running data quality check')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        results = redshift.get_records(self.sql_query)
        
        if len(results) < 1 or len(results[0]) < 1:
            self.log.info("data quality check passed: no records")
        else:
            raise ValueError(f"Data quality check failed. {self.table} returned results")
