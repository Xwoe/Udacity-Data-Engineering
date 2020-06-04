from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {table}
        {select_sql};
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 select_sql='',
                 table='',    
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.table = table

    def execute(self, context):
        
        formatted_sql = LoadFactOperator.insert_sql.format(
            table=self.table,
            select_sql=self.select_sql
        )
        self.log.info(f'Appending data to {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run(formatted_sql)

        