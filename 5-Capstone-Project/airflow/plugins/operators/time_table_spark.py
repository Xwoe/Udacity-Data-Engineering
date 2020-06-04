from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


class TimeSparkOperator(BaseOperator):
    ui_color = '#008140'

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 #s3_bucket='',
                 #s3_key='',
                 table='dt_time',
                 aws_credentials_id='',
                 region='us-west-2',
                 
                 *args, **kwargs):

        super(TimeSparkOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.redshift_conn_id = redshift_conn_id
        #self.s3_bucket = s3_bucket
        #self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.region = region

    def execute(self, context):
        self.log.info('TimeSparkOperator initializing...')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        redshift_con_string = "jdbc:redshift://{}:{}/{}".format(
            redshift.host, redshift.port, redshift.schema    
            )
        
        
        movies_df.write \
             .format("jdbc")  \
             .option("url", redshift_con_string) \
             .option("dbtable", "public.dt_time") \
             .option("user", redshift.user) \
             .option("password", redshift.password) \
             .option("driver", "com.amazon.redshift.jdbc42.Driver") \
             .mode("append") \
             .save()
        
    def create_full_time_table(outputfolder, daysafter=36525):

        future_days = range(daysafter)
        all_dates = [(t,) for t in future_days]

        t_schema = T.StructType([T.StructField('i_date', T.IntegerType())])
        timeframe = spark.createDataFrame(all_dates, t_schema)
        timeframe = timeframe.withColumn("dt_date", F.expr("date_add(to_date('1960-01-01'), i_date)"))
        timeframe = timeframe.select('i_date', 'dt_date',
                        F.year('dt_date').alias('year'),
                        F.month('dt_date').alias('month'),
                        F.dayofmonth('dt_date').alias('day'),
                        F.dayofweek('dt_date').alias('weekday'))

        #timeframe.write.partitionBy('year', 'month').parquet(os.path.join(outputfolder, 'dates.parquet'), 'overwrite')
        return timeframe
    
    def get_spark_sesssion():
        
        spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
            .enableHiveSupport().getOrCreate()
        return spark