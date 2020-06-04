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


def create_country_table(spark, datafolder, outputfolder):
    logger.info('creating country table')
    cntyl = utils.spark_read_csv(spark, datafolder, 'cntyl.csv', sep=',', quotechar='"')
    cntyl.write.parquet(os.path.join(outputfolder, 'country.parquet'), 'overwrite')
    logger.info('country.parquet written')
    return cntyl

def create_full_time_table(spark, outputfolder):
    """
    This creates a full time table until the year 2060. This should reduce the processing
    time a bit, since the dates don't have to processed each time new facts are added
    """
    logger.info('Creating dates table')
    days_till_2060 = range(int(100 * 365.25))
    all_dates = [(t,) for t in days_till_2060]

    t_schema = T.StructType([T.StructField('i94_date', T.IntegerType())])
    timeframe = spark.createDataFrame(all_dates, t_schema)
    timeframe = timeframe.withColumn("dt_date", F.expr("date_add(to_date('1960-01-01'), i94_date)"))
    timeframe = timeframe.select('i94_date', 'dt_date',
                    F.year('dt_date').alias('year'),
                    F.month('dt_date').alias('month'),
                    F.dayofmonth('dt_date').alias('day'),
                    F.dayofweek('dt_date').alias('weekday'))

    timeframe.write.partitionBy('year', 'month').parquet(os.path.join(outputfolder, 'dates.parquet'), 'overwrite')
    logger.info('dates.parquet written')
    return timeframe


def generate_temperature_country(spark, datafolder, outputfolder):

    logger.info('Creating temperature_country')
    climate = utils.spark_read_csv(spark, datafolder, 'GlobalLandTemperaturesByCountry.csv', sep=',')
    climate = climate.withColumn('dt', F.to_date(F.col('dt')))
    country = spark.read.option("mergeSchema", "true").parquet(os.path.join(outputfolder, 'country.parquet'))
    climate = climate.join(country, on=country['cntyl'] == climate['Country'], how='leftouter')
    climate = climate.drop('i94cntyl')
    climate = utils.rename_columns(climate)
    climate = climate.withColumn('year', F.year('dt').alias('year'))
    #withColumnRenamed('id', 'country_id').
    climate = climate.\
        withColumnRenamed('averagetemperatureuncertainty', 'avg_uncertainty').\
        withColumnRenamed('averagetemperature', 'avg_temperature')
    climate.write.partitionBy('year', 'country').parquet(os.path.join(
        outputfolder, 'temperature_country.parquet'), 'overwrite')
    logger.info('temperature_country.parquet written')
    return climate

def create_annual_temp_table(spark, outputfolder):
    logger.info('Creating temperature_annual_country')
    annual = spark.read.option("mergeSchema", "true").parquet(os.path.join(outputfolder, 'temperature_country.parquet'))
    annual.groupby([F.col('country'), F.col('cntyl_id'), F.col('year')]).\
        agg(F.avg('avg_temperature').alias('avg_temperature'))
    annual.write.partitionBy('country').parquet(os.path.join(outputfolder, 'temperature_annual_country.parquet'), 'overwrite')
    logger.info('temperature_annual_country written')
    return annual


def create_demographic_table(spark, datafolder, outputfolder):
    logger.info('creating city_demographics')
    # select a subset of original table
    df_demo = utils.spark_read_csv(spark, datafolder, 'prtl_city.csv')
    demographics = utils.spark_read_csv(spark, datafolder, 'us-cities-demographics.csv', sep=';')

    # do the preprocessing, append columns
    df_demo = df_demo.withColumnRenamed('prtl_city', 'port_city')
    df_demo = utils.map_col(spark, df_demo, datafolder, 'prtl_state', 'prtl_city_id', 'port_state_short')
    df_demo = utils.map_col(spark, df_demo, datafolder, 'addrl', 'port_state_short', 'port_state')

    # we are deleting the ones, for which no states were found, since we only focus on the US here
    # later expand world wide
    df_demo = df_demo.dropna(subset=['port_state'])
    df_demo = df_demo.join(demographics, (df_demo.port_city == demographics.City) & (df_demo.port_state_short == demographics['State Code']))
    columns_to_drop = ['port_city', 'port_state', 'port_state_short']
    df_demo = df_demo.drop(*columns_to_drop)

    # reformat column names
    df_demo = utils.rename_columns(df_demo)
    df_demo.write.partitionBy('state').parquet(os.path.join(outputfolder, 'city_demographics.parquet'), 'overwrite')
    logger.info('city_demographics written')
    return df_demo


def main(input_data, output_data):

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

    create_country_table(spark, csv_folder, output_data)
    create_full_time_table(spark, output_data)

    # country table
    utils.csv_to_parquet(spark, csv_folder, output_data, 'cntyl', 'country')
    # mode of transport
    utils.csv_to_parquet(spark, csv_folder, output_data, 'model', 'transport')
    # type of visa
    utils.csv_to_parquet(spark, csv_folder, output_data, 'visa', 'visa')
    # city demographics
    create_demographic_table(spark, csv_folder, output_data)

    # global temperatures
    utils.csv_to_parquet(spark, temperature_folder, output_data, 'GlobalTemperatures', 'temperature_global')
    # country temperatures
    generate_temperature_country(spark, temperature_folder, output_data)
    #utils.csv_to_parquet(spark, temperature_folder, output_data, 'GlobalLandTemperaturesByCountry', 'temperature_country')
    create_annual_temp_table(spark, output_data)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--inputfolder', default='s3a://xwoe-udacity/deng_capstone/')
    parser.add_argument('-o', '--outputfolder', default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder)
