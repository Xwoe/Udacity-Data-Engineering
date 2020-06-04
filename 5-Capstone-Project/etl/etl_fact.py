import argparse
from datetime import datetime
import os
#import configparser

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, \
    StringType as Str, IntegerType as Int, LongType as Long, DateType as Date, TimestampType as Ts
import pyspark.sql.functions as F
import pyspark.sql.types as T

from log import get_logger
logger = get_logger(__name__)
import utils


#def create_spark_session(aws_key, aws_secret_key):
def create_spark_session():
    """
    Create a new Spark session and return it.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

    """
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",
                                                      "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "100")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.maximum", "5000")
    """
    return spark


def convert_datatypes(df_spark):
    return df_spark.withColumn('i94yr', df_spark['i94yr'].cast(T.IntegerType())).\
            withColumn('i94mon', df_spark['i94mon'].cast(T.IntegerType())).\
            withColumn('i94cit', df_spark['i94cit'].cast(T.IntegerType())).\
            withColumn('i94res', df_spark['i94res'].cast(T.IntegerType())).\
            withColumn('arrdate', df_spark['arrdate'].cast(T.IntegerType())).\
            withColumn('i94mode', df_spark['i94mode'].cast(T.IntegerType())).\
            withColumn('depdate', df_spark['depdate'].cast(T.IntegerType())).\
            withColumn('i94bir', df_spark['i94bir'].cast(T.IntegerType())).\
            withColumn('i94visa', df_spark['i94visa'].cast(T.IntegerType())).\
            withColumn('count', df_spark['count'].cast(T.IntegerType())).\
            withColumn('biryear', df_spark['biryear'].cast(T.IntegerType())).\
            withColumn('admnum', df_spark['admnum'].cast(T.IntegerType()))


def create_fact_table(spark, input_folder, output_folder):

    df_spark = spark.read.option("mergeSchema", "true").parquet(os.path.join(DATAFOLDER, 'sas_data'))
    df_fact = df_test.withColumn("arrival_dt", F.expr("date_add(to_date('1960-01-01'), arrdate)"))
    df_fact = df_fact.withColumn("depart_dt", F.expr("date_add(to_date('1960-01-01'), depdate)"))




def main(input_folder, output_folder):

    #input_data = "s3a://xwoe-udacity/deng_capstone/"
    #output_data = "s3://xwoe-udacity/deng_capstone/tables/"

    csv_folder = input_data + '/csvs/'
    temperature_folder = os.path.join(input_data, 'temperature')
    # CONFIG
    #config = configparser.ConfigParser()
    #config.read('dl.cfg')

    #aws_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    #aws_secret_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    spark = create_spark_session()




if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inputfolder', default='s3a://xwoe-udacity/deng_capstone/')
    parser.add_argument('-o', '--outputfolder', default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder)
