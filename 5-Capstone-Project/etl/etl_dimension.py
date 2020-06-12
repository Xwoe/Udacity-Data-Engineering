import argparse
from datetime import datetime
import os
import configparser

from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F
import pyspark.sql.types as T

from log import get_logger
logger = get_logger(__name__)
import utils


def generate_country_table(spark, input_folder, output_folder):
    """
    Bootstrapper function of all the preprocessing steps.

    Parameters
    ----------
    spark: SparkSession
    input_folder : str
        Folder path of unprocessed parquet file.
    output_folder : str
        Folder path of processed parquet file.
    """
    logger.info('creating country table')
    cntyl = utils.spark_read_csv(spark, input_folder, 'cntyl.csv', sep=',', quotechar='"')
    cntyl.write.parquet(os.path.join(output_folder, 'country.parquet'), 'overwrite')
    logger.info('country.parquet written')

def generate_full_time_table(spark, output_folder):
    """
    This creates a full time table until the year 2060. This should reduce the processing
    time a bit, since the dates don't have to processed each time new facts are added

    Parameters
    ----------
    spark: SparkSession
    output_folder : str
        Folder path of processed parquet file.
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

    timeframe.write\
        .parquet(os.path.join(output_folder, 'dates.parquet'), 'overwrite')
    logger.info('dates.parquet written')

def generate_temperature_country(spark, input_folder, output_folder):
    """
    Create table with monthly temperatures per country.

    Parameters
    ----------
    spark: SparkSession
    input_folder : str
        Folder path of unprocessed parquet file.
    output_folder : str
        Folder path of processed parquet file.
    """
    logger.info('Creating temperature_country')
    climate = utils.spark_read_csv(spark, input_folder, 'GlobalLandTemperaturesByCountry.csv', sep=',')
    climate = climate.withColumn('dt', F.to_date(F.col('dt')))
    country = spark.read.option("mergeSchema", "true").parquet(os.path.join(output_folder, 'country.parquet'))
    climate = climate.join(country, on=country['cntyl'] == climate['Country'], how='leftouter')
    climate = climate.drop('cntyl')
    climate = utils.rename_columns(climate)
    climate = climate.withColumn('year', F.year('dt').alias('year'))

    double_cols = ['average_temperature_uncertainty', 'average_temperature']
    climate = utils.round_columns(climate, double_cols, 3)

    climate.write\
        .parquet(os.path.join(
            output_folder, 'temperature_country.parquet'), 'overwrite')
    logger.info('temperature_country.parquet written')

def generate_annual_temp_table(spark, output_folder):
    """
    Aggregate temperatures to annual values per country.

    Parameters
    ----------
    spark: SparkSession
    output_folder : str
        Folder path of processed parquet file.
    """

    logger.info('Creating temperature_annual_country')
    annual = spark.read.option("mergeSchema", "true")\
        .parquet(os.path.join(output_folder, 'temperature_country.parquet'))
    annual = annual.groupby([F.col('country'), F.col('cntyl_id'), F.col('year')]).\
        agg(F.avg('average_temperature').alias('average_temperature'))
    double_cols = ['average_temperature']
    annual = utils.round_columns(annual, double_cols, 3)
    annual.write\
        .parquet(os.path.join(output_folder, 'temperature_annual_country.parquet'), 'overwrite')
    logger.info('temperature_annual_country.parquet written')


def generate_global_temp_table(spark, input_folder, output_folder):
    """
    Create table for global annual temperatures.

    Parameters
    ----------
    spark: SparkSession
    input_folder : str
        Folder path of unprocessed parquet file.
    output_folder : str
        Folder path of processed parquet file.
    """

    logger.info('Creating temperature_global')
    climate = utils.spark_read_csv(spark, input_folder, 'GlobalTemperatures.csv', sep=',')
    climate = utils.rename_columns(climate)
    climate = climate.withColumn('year', F.year('dt').alias('year'))
    double_cols = ['land_average_temperature',
        'land_average_temperature_uncertainty',
        'land_max_temperature',
        'land_max_temperature_uncertainty',
        'land_min_temperature',
        'land_min_temperature_uncertainty',
        'land_and_ocean_average_temperature',
        'land_and_ocean_average_temperature_uncertainty']
    agg_dict = dict(zip(double_cols, ['avg'] * len(double_cols)))
    climate = climate.groupby([F.col('year')]).\
        agg(agg_dict)
    all_col_names = ['year'] + double_cols
    climate = climate.toDF(*all_col_names)

    climate = utils.round_columns(climate, double_cols, 3)

    climate.write.parquet(os.path.join(
        output_folder, 'temperature_global.parquet'), 'overwrite')

    logger.info('temperature_global.parquet written')

def generate_demographic_table(spark, input_folder, output_folder):
    """
    Create table for demographic city data.

    Parameters
    ----------
    spark: SparkSession
    input_folder : str
        Folder path of unprocessed parquet file.
    output_folder : str
        Folder path of processed parquet file.
    """

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
    df_demo = df_demo.join(demographics, (df_demo.port_city == demographics.City) & \
                                         (df_demo.port_state_short == demographics['State Code']))
    columns_to_drop = ['port_city', 'port_state', 'port_state_short']
    df_demo = df_demo.drop(*columns_to_drop)

    # reformat column names
    df_demo = utils.rename_columns(df_demo)

    # Reduce columns and remove duplicates, since there is one entry for each race.
    # Race and count are the columns for each race's count.
    keep_columns = ['prtl_city_id', 'city', 'median_age', 'male_population', 'female_population',
                    'total_population', 'foreignborn', 'average_household_size', 'state_code',
                    'state']
    df_demo = df_demo.select(keep_columns)
    df_demo = df_demo.dropDuplicates()

    df_demo.write\
        .parquet(os.path.join(output_folder, 'city_demographics.parquet'), 'overwrite')
    logger.info('city_demographics written')

def main(input_folder, output_folder, sthree):
    """
    Start preprocessing either locally or on a remote S3 folder.
    """

    if sthree:
        config = configparser.ConfigParser()
        config.read('dl.cfg')
        aws_key = config.get('AWS', 'AWS_ACCESS_KEY_ID')
        aws_secret_key = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')
        spark = utils.create_sthree_spark_session(aws_key, aws_secret_key)

    else:
        spark = utils.create_spark_session()


    csv_folder = input_folder + '/csvs/'
    temperature_folder = os.path.join(input_folder, 'temperature')

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

    ## TEMPERATURES
    # global temperatures
    generate_global_temp_table(spark, temperature_folder, output_folder)
    # country temperatures
    generate_temperature_country(spark, temperature_folder, output_folder)

    # annual average temperature for each country.
    generate_annual_temp_table(spark, output_folder)

if __name__ == "__main__":
    """
    The flag -s shall be used if a remote S3 storage is being used, which requires credentials.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true')
    parser.add_argument('-i', '--inputfolder', default='s3a://xwoe-udacity/deng_capstone/')
    parser.add_argument('-o', '--outputfolder', default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder, args.s)
