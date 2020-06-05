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

def generate_country_table(spark, input_folder, output_folder):
    logger.info('creating country table')
    cntyl = utils.spark_read_csv(spark, input_folder, 'cntyl.csv', sep=',', quotechar='"')
    cntyl.write.parquet(os.path.join(output_folder, 'country.parquet'), 'overwrite')
    logger.info('country.parquet written')
    return cntyl

def generate_full_time_table(spark, output_folder):
    """
    This creates a full time table until the year 2060. This should reduce the processing
    time a bit, since the dates don't have to processed each time new facts are added
    """
    logger.info('Creating dates table')
    days_till_2060 = range(int(100 * 365.25))
    all_dates = [(t,) for t in days_till_2060]

    t_schema = T.StructType([T.StructField('i_date', T.IntegerType())])
    timeframe = spark.createDataFrame(all_dates, t_schema)
    timeframe = timeframe.withColumn("dt_date", F.expr("date_add(to_date('1960-01-01'), i_date)"))
    timeframe = timeframe.select('i_date', 'dt_date',
                    F.year('dt_date').alias('year'),
                    F.month('dt_date').alias('month'),
                    F.dayofmonth('dt_date').alias('day'),
                    F.dayofweek('dt_date').alias('weekday'))

    timeframe.write.partitionBy('year', 'month').parquet(os.path.join(output_folder, 'dates.parquet'), 'overwrite')
    logger.info('dates.parquet written')
    return timeframe


def generate_temperature_country(spark, input_folder, output_folder):

    logger.info('Creating temperature_country')
    climate = utils.spark_read_csv(spark, input_folder, 'GlobalLandTemperaturesByCountry.csv', sep=',')
    climate = climate.withColumn('dt', F.to_date(F.col('dt')))
    country = spark.read.option("mergeSchema", "true").parquet(os.path.join(output_folder, 'country.parquet'))
    climate = climate.join(country, on=country['cntyl'] == climate['Country'], how='leftouter')
    climate = climate.drop('i94cntyl')
    climate = utils.rename_columns(climate)
    climate = climate.withColumn('year', F.year('dt').alias('year'))
    #withColumnRenamed('id', 'country_id').
    climate = climate.\
        withColumnRenamed('averagetemperatureuncertainty', 'avg_uncertainty').\
        withColumnRenamed('averagetemperature', 'avg_temperature')
    climate.write.partitionBy('year', 'country').parquet(os.path.join(
        output_folder, 'temperature_country.parquet'), 'overwrite')
    logger.info('temperature_country.parquet written')
    return climate

def generate_annual_temp_table(spark, output_folder):
    logger.info('Creating temperature_annual_country')
    annual = spark.read.option("mergeSchema", "true").parquet(os.path.join(output_folder, 'temperature_country.parquet'))
    annual.groupby([F.col('country'), F.col('cntyl_id'), F.col('year')]).\
        agg(F.avg('avg_temperature').alias('avg_temperature'))
    annual.write.partitionBy('country').parquet(os.path.join(output_folder, 'temperature_annual_country.parquet'), 'overwrite')
    logger.info('temperature_annual_country written')
    return annual


def generate_demographic_table(spark, input_folder, output_folder):
    logger.info('creating city_demographics')
    # select a subset of original table
    df_demo = utils.spark_read_csv(spark, input_folder, 'prtl_city.csv')
    demographics = utils.spark_read_csv(spark, input_folder, 'us-cities-demographics.csv', sep=';')

    # do the preprocessing, append columns
    df_demo = df_demo.withColumnRenamed('prtl_city', 'port_city')
    df_demo = utils.map_col(spark, df_demo, input_folder, 'prtl_state', 'prtl_city_id', 'port_state_short')
    df_demo = utils.map_col(spark, df_demo, input_folder, 'addrl', 'port_state_short', 'port_state')

    # we are deleting the ones, for which no states were found, since we only focus on the US here
    # later expand world wide
    df_demo = df_demo.dropna(subset=['port_state'])
    df_demo = df_demo.join(demographics, (df_demo.port_city == demographics.City) & (df_demo.port_state_short == demographics['State Code']))
    columns_to_drop = ['port_city', 'port_state', 'port_state_short']
    df_demo = df_demo.drop(*columns_to_drop)

    # reformat column names
    df_demo = utils.rename_columns(df_demo)
    df_demo.write.partitionBy('state').parquet(os.path.join(output_folder, 'city_demographics.parquet'), 'overwrite')
    logger.info('city_demographics written')
    return df_demo


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


    csv_folder = input_folder + '/csvs/'
    temperature_folder = os.path.join(input_folder, 'temperature')
    # CONFIG
    #config = configparser.ConfigParser()
    #config.read('dl.cfg')

    #aws_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    #aws_secret_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    spark = create_spark_session()

    generate_country_table(spark, csv_folder, output_folder)
    generate_full_time_table(spark, output_folder)

    # country table
    utils.csv_to_parquet(spark, csv_folder, output_folder, 'cntyl', 'country')
    # mode of transport
    utils.csv_to_parquet(spark, csv_folder, output_folder, 'model', 'transport')
    # type of visa
    utils.csv_to_parquet(spark, csv_folder, output_folder, 'visa', 'visa')
    # city demographics
    generate_demographic_table(spark, csv_folder, output_folder)

    # global temperatures
    utils.csv_to_parquet(spark, temperature_folder, output_folder, 'GlobalTemperatures', 'temperature_global')
    # country temperatures
    generate_temperature_country(spark, temperature_folder, output_folder)
    #utils.csv_to_parquet(spark, temperature_folder, output_folder, 'GlobalLandTemperaturesByCountry', 'temperature_country')
    generate_annual_temp_table(spark, output_folder)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true')
    parser.add_argument('-i', '--inputfolder', default='s3a://xwoe-udacity/deng_capstone/')#'s3a://xwoe-udacity/deng_capstone/immigration/sas_data')
    parser.add_argument('-o', '--outputfolder', default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder, args.s)
