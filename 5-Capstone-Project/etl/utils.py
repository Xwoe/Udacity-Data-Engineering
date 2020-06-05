from functools import reduce
from itertools import chain
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
import pyspark.sql.types as T
import os

def process_time(df_time):
    df_time = df_time.withColumn("arrival_date", F.expr("date_add(to_date('1960-01-01'), arrdate)"))
    df_time = df_time.withColumn("depart_date", F.expr("date_add(to_date('1960-01-01'), depdate)"))
    return df_time.withColumn("diff_days", F.datediff("depart_date", "arrival_date")).show()


def format_allcaps(st):
    """
    Run `capitalize()' on each word in a string separated by blanks.
    """
    spl = st.split(' ')
    spl = [s.capitalize() for s in spl]
    return ' '.join(spl)


def map_col(spark, df, datafolder, map_col_name, df_col_name, new_col_name):
    """


    Parameters
    ----------
    df : spark dataframe
        The file containing the df_col_name to be used for mapping.
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


@udf
def udf_city_name(city_full):
    """
    Format the values for city names to be properly capitalized word by word.
    """
    return str.split(city_full, ',')[0].capitalize()

def format_column_names(s):
    s = s.casefold()
    s = s.replace(' ', '_')
    s = s.replace('-', '_')
    s = s.replace('i94', 'i_')
    return s

def rename_columns(df):
    old_names = df.schema.names
    new_names = [format_column_names(s) for s in old_names]
    df = reduce(lambda df, idx:
                df.withColumnRenamed(old_names[idx], new_names[idx]), range(len(old_names)), df)
    return df


def spark_read_csv(spark, folder, filename, **kwargs):

    return spark.read.format('csv').options(header='true', inferSchema=True, **kwargs).\
        load(os.path.join(folder, filename))

def csv_to_parquet(spark, datafolder, outputfolder, csv_name, table_name):
    df = spark_read_csv(spark, datafolder, f'{csv_name}.csv', sep=',', quotechar=['"'])
    #df = df.withColumnRenamed('value', 'id')
    df.write.parquet(os.path.join(outputfolder, f'{table_name}.parquet'), 'overwrite')
    return df