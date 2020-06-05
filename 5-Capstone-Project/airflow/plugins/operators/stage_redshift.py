from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



"""
Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. 
The operator creates and runs a SQL COPY statement based on the parameters provided. 
The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
"""

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {table} 
        FROM '{s3_path}' 
        IAM_ROLE '{iam}'
        {file_format};
        """
    
    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 s3_bucket='',
                 s3_key='',
                 aws_iam='',
                 aws_credentials_id='',
                 region='us-west-2',
                 file_format='FORMAT AS PARQUET',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_iam = aws_iam
        self.aws_credentials_id=aws_credentials_id
        self.region = region
        self.file_format = file_format

    def execute(self, context):
        self.log.info('StageToRedshiftOperator initializing...')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            iam=self.aws_iam,
            region=self.region,
            file_format=self.file_format   
        )
        redshift.run(formatted_sql)


        
        

