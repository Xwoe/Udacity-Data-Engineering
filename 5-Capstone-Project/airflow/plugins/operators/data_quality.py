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
                 check_not_empty=False,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.expected_result = expected_result
        self.check_not_empty = check_not_empty
        
    def execute(self, context):
        
        self.log.info('Running data quality check')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        results = redshift.get_records(self.sql_query)
        
        if len(results) < 1 or len(results[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        num_records = results[0][0] 
        if self.check_not_empty:
            if num_records < 1:
                raise ValueError(f"Data quality check failed. Table is empty")
                self.log.info("data quality check failed: no records")
            else:
                self.log.info("data quality check passed")
        
        else:
            if  num_records != self.expected_result:
                raise ValueError(f"Data quality check failed. Null values found")
                self.log.info("data quality check failed")
            else:
                self.log.info("data quality check passed")