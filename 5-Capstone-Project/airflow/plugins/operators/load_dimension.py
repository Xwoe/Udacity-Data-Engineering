from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {table}
        {select_sql};
    """
        
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 select_sql='',
                 table='',
                 insert_mode='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.table = table
        self.insert_mode = insert_mode

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.insert_mode == 'flush':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f'Inserting into table {self.table}')
        
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            table=self.table,
            select_sql=self.select_sql
        )

        redshift.run(formatted_sql)
        
            
        