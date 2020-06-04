from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 sql_query='',
                 expected_result=0,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.expected_result = expected_result
        
    def execute(self, context):
        
        self.log.info('Running data quality check')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        results = redshift.get_records(self.sql_query)
        num_records = results[0][0] 
        if  num_records != self.expected_result:
            raise ValueError(f"Data quality check failed. Null values found")
            self.log.info("data quality check failed")
        else:
            self.log.info("data quality check passed")