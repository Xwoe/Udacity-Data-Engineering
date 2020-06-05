import argparse
from datetime import datetime
import os
import configparser

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



def create_spark_session():
#def create_spark_session():
    """
    Create a new Spark session and return it.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()


    logger.info('using local data store')

    return spark

def create_sthree_spark_session(aws_key, aws_secret_key):
    """
    Create a new Spark session and return it.
    """
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.5" pyspark-shell'
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()


    logger.info('using S3 data store')
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl",
                                                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "100")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.maximum", "5000")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.buffer.dir", "/root/spark/work,/tmp")

    return spark


def read_immigration(spark, input_folder):

    logger.info('read immigration data')
    df = spark.read.option("mergeSchema", "true").parquet(input_folder)
    logger.info('convert data types')
    df = convert_datatypes(df)
    # replace all i94 with i_
    df = utils.rename_columns(df)
    return df


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


def add_duration_column(df):

    logger.info('adding duration column')
    df = df.withColumn("arrival_dt", F.expr("date_add(to_date('1960-01-01'), arrdate)"))
    df = df.withColumn("depart_dt", F.expr("date_add(to_date('1960-01-01'), depdate)"))
    return df.withColumn("length_stay", F.datediff("depart_dt", "arrival_dt"))


def remove_colums(df):
    logger.info('removing unused columns')
    keep_columns = ['i_yr', 'i_mon', 'arrdate', 'depdate', 'i_cit', 'i_res', 'i_port',
                    'i_mode', 'i_addr', 'i_bir', 'i_visa', 'visatype',
                    'gender', 'airline', 'fltno', 'length_stay'] #'biryear', 'cicid'
    return df.select(keep_columns)


# TODO switch back to 'append' in the end
def write_immigration(spark, df, output_folder):
    logger.info('writing immigration.parquet')
    df.write.partitionBy('i_yr', 'i_mon')\
            .mode('overwrite')\
            .parquet(os.path.join(output_folder, 'immigration.parquet'))
    logger.info('immigration.parquet written')

def create_immigration_table(spark, input_folder, output_folder):

    # TODO get the latest month from the parquet file and get only the updated files
    # from the staging parquet
    df = read_immigration(spark, input_folder)
    df = add_duration_column(df)
    df = remove_colums(df)
    write_immigration(spark, df, output_folder)





def main(input_folder, output_folder, sthree):


    if sthree:
        config = configparser.ConfigParser()
        config.read('dl.cfg')

        #os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        #os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
        aws_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
        aws_secret_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
        spark = create_sthree_spark_session(aws_key, aws_secret_key)

    else:
        spark = create_spark_session()

    create_immigration_table(spark, input_folder, output_folder)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true')
    parser.add_argument('-i', '--inputfolder', default='s3a://xwoe-udacity/deng_capstone/immigration/sas_data')#'s3a://xwoe-udacity/deng_capstone/immigration/sas_data')
    parser.add_argument('-o', '--outputfolder', default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder, args.s)
