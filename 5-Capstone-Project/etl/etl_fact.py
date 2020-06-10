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



def read_immigration(spark, input_folder):
    """
    Read the immigration parquet file, apply schema and rename columns.

    Parameters
    ----------

    spark: SparkSession
    input_folder : str
        Path to immigration data folder.
    """
    logger.info('read immigration data')
    df = spark.read.option("mergeSchema", "true").parquet(input_folder)
    logger.info('convert data types')
    df = convert_datatypes(df)
    # replace all i94 with i_
    df = utils.rename_columns(df)
    return df


def convert_datatypes(df):
    """
    Convert column types.

    Parameters
    ----------
    df: pyspark.sql.dataframe.DataFrame
    """
    return df.withColumn('i94yr', df['i94yr'].cast(T.IntegerType())).\
            withColumn('i94mon', df['i94mon'].cast(T.IntegerType())).\
            withColumn('i94cit', df['i94cit'].cast(T.IntegerType())).\
            withColumn('i94res', df['i94res'].cast(T.IntegerType())).\
            withColumn('arrdate', df['arrdate'].cast(T.IntegerType())).\
            withColumn('i94mode', df['i94mode'].cast(T.IntegerType())).\
            withColumn('depdate', df['depdate'].cast(T.IntegerType())).\
            withColumn('i94bir', df['i94bir'].cast(T.IntegerType())).\
            withColumn('i94visa', df['i94visa'].cast(T.IntegerType())).\
            withColumn('count', df['count'].cast(T.IntegerType())).\
            withColumn('biryear', df['biryear'].cast(T.IntegerType())).\
            withColumn('cicid', df['cicid'].cast(T.IntegerType())).\
            withColumn('admnum', df['admnum'].cast(T.IntegerType()))


def add_duration_column(df):
    """
    Calculate the duration between arrival date and departement date and store in column.

    Parameters
    ----------
    df: pyspark.sql.dataframe.DataFrame
    """
    logger.info('adding duration column')
    df = df.withColumn("arrival_dt", F.expr("date_add(to_date('1960-01-01'), arrdate)"))
    df = df.withColumn("depart_dt", F.expr("date_add(to_date('1960-01-01'), depdate)"))
    return df.withColumn("length_stay", F.datediff("depart_dt", "arrival_dt"))


def remove_colums(df):
    """
    Select columns to keep.

    Parameters
    ----------
    df: pyspark.sql.dataframe.DataFrame
    """
    logger.info('removing unused columns')
    keep_columns = ['cicid', 'i_yr', 'i_mon', 'arrdate', 'depdate', 'i_cit', 'i_res', 'i_port',
                    'i_mode', 'i_addr', 'i_bir', 'i_visa', 'visatype',
                    'gender', 'airline', 'fltno', 'length_stay']
    return df.select(keep_columns)


def write_immigration(spark, df, output_folder):
    """
    Write parquet file to output folder.

    Parameters
    ----------
    spark: SparkSession
    df: pyspark.sql.dataframe.DataFrame
    output_folder : str
        Folder path of processed parquet file.
    """
    logger.info('writing immigration.parquet')
    df.write\
            .mode('overwrite')\
            .parquet(os.path.join(output_folder, 'immigration.parquet'))
    logger.info('immigration.parquet written')


def create_immigration_table(spark, input_folder, output_folder):
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
    df = read_immigration(spark, input_folder)
    df = add_duration_column(df)
    df = remove_colums(df)
    write_immigration(spark, df, output_folder)

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

    create_immigration_table(spark, input_folder, output_folder)


if __name__ == "__main__":
    """
    The flag -s shall be used if a remote S3 storage is being used, which requires credentials.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', action='store_true')
    parser.add_argument('-i', '--inputfolder',
                        default='s3a://xwoe-udacity/deng_capstone/immigration/sas_data')
    parser.add_argument('-o', '--outputfolder',
                        default='s3://xwoe-udacity/deng_capstone/tables/')
    args = parser.parse_args()

    main(args.inputfolder, args.outputfolder, args.s)
