import os
import re
from functools import reduce
from itertools import chain

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf

from log import get_logger
logger = get_logger(__name__)

def create_spark_session():
    """
    Create a new Spark session and return it.
    This function shall be used if either the script is being run on a local machine
    accessing local data sources or if it is being run within an EMR instance.
    """
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()

    logger.info('using local data store')

    return spark

def create_sthree_spark_session(aws_key, aws_secret_key):
    """
    Create a new Spark session and return it.
    This function shall be used if the script accesses a remote S3 instance and
    credentials are required.
    """
    os.environ['PYSPARK_SUBMIT_ARGS'] = \
        '--packages "org.apache.hadoop:hadoop-aws:2.7.5" pyspark-shell'
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


def map_col(spark, df, datafolder, map_col_name, df_col_name, new_col_name):
    """
    Map the mapping of a simple csv file with key-value structure to a new column in the dataframe
    matching the keys.

    Parameters
    ----------
    spark: SparkSession
    df : spark dataframe
        The file containing the df_col_name to be used for mapping.
    datafolder : str
        Folder location of the csv file to be used for mapping.
    map_col_name : str
        The column name of the mapping file.
    df_col_name : str
        The column name in the Spark dataframe to be used.
    new_col_name : str
        New column name of the mapping results.
    """
    df_map = spark_read_csv(spark, datafolder, f'{map_col_name}.csv')
    df_map = df_map.toPandas()
    id_col = f'{map_col_name}_id'
    dic_map = dict(zip(df_map[id_col], df_map[map_col_name]))
    mapping_expr = F.create_map([F.lit(x) for x in chain(*dic_map.items())])
    return df.withColumn(new_col_name, mapping_expr[F.col(df_col_name)])

def camel_to_snake(s):
    """
    Transforms CamelCase notation to snake_case notation.
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()


def format_column_names(s):
    """
    Format the column names to be proper names, which can also be used in a SQL table.
    """
    s = s.replace(' ', '')
    s = s.replace('-', '')
    s = s.replace('i94', 'i_')
    s = camel_to_snake(s)
    return s

def rename_columns(df):
    """
    Rename columns in dataframe to match SQL standards.

    Parameters
    ----------
    df : spark dataframe
        The file containing the df_col_name to be used for mapping.
    """
    old_names = df.schema.names
    new_names = [format_column_names(s) for s in old_names]
    df = reduce(lambda df, idx:
                df.withColumnRenamed(old_names[idx], new_names[idx]), range(len(old_names)), df)
    return df

def round_columns(df, columns, num_decimals):
    """
    Round columns to the specified number of decimal places.

    Parameters
    ----------
    df : spark dataframe
        The file containing the df_col_name to be used for mapping.
    columns : [str]
        List of columns, on which the mapping should be applied.
    num_decimals : int
        Number of decimals to use for rounding.
    """
    for col in columns:
        df = df.withColumn(col, F.round(df[col], num_decimals))
    return df


def spark_read_csv(spark, folder, filename, **kwargs):
    """
    Round columns to the specified number of decimal places.

    Parameters
    ----------
    spark: SparkSession
    folder : str
        Input folder name.
    filename : str
    """
    return spark.read.format('csv').options(header='true', inferSchema=True, **kwargs).\
        load(os.path.join(folder, filename))

def csv_to_parquet(spark, datafolder, outputfolder, csv_name, table_name):
    """
    Read contents of a csv files and write them to a parquet file.

    Parameters
    ----------
    spark : SparkSession
    datafolder : str
        Input folder for csv file.
    outputfolder : str
        Output folder for parquet file.
    csv_name : str
        Name of csv file without the .csv ending.
    table_name : str
        Name of the output parquet file.
    """
    df = spark_read_csv(spark, datafolder, f'{csv_name}.csv', sep=',', quotechar=['"'])
    df.write.parquet(os.path.join(outputfolder, f'{table_name}.parquet'), 'overwrite')
    return df