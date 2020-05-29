from itertools import chain
from pyspark.sql.functions import udf


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

def process_time(df_time)
    df_time = df_time.withColumn("arrival_date", F.expr("date_add(to_date('1960-01-01'), arrdate)"))
    df_time = df_time.withColumn("depart_date", F.expr("date_add(to_date('1960-01-01'), depdate)"))
    return df_time.withColumn("diff_days", F.datediff("depart_date", "arrival_date")).show()

    #TODO
    # aggregate the stuff to the date table
    # probably better to do:
        # first swoop: create date table with all the stuff from unique values
        # second swoop: create duration via lookup

def format_allcaps(st):
    """
    Run `capitalize()' on each word in a string separated by blanks.
    """
    spl = st.split(' ')
    spl = [s.capitalize() for s in spl]
    return ' '.join(spl)


def map_col(df, map_col_name, df_col_name, new_col_name):
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
    df_map = pd.read_csv(os.path.join(DATAFOLDER, f'{map_col_name}.csv'), quotechar="'")
    dic_map = dict(zip(df_map['value'], df_map[map_col_name]))
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
    return s

def rename_columns(df):
    old_names = df.schema.names
    new_names = [format_column_names(s) for s in old_names]
    df = reduce(lambda df, idx:
                df.withColumnRenamed(old_names[idx], new_names[idx]), range(len(old_names)), df)
    return df

def create_demographic_table(df, demographics, outputfolder):
    # select a subset of original table
    df_demo = df.select(['i94port']).dropDuplicates()
    # do the preprocessing, append columns
    df_demo = map_col(df_demo, 'i94prtl_city', 'i94port', 'port_city')
    #df_test = df_test.withColumn("port_state_short", udf_state_short("port"))
    df_demo = map_col(df_demo, 'i94prtl_state', 'i94port', 'port_state_short')
    df_demo = map_col(df_demo, 'i94addrl', 'port_state_short', 'port_state')
    # we are deleting the ones, for which no states were found, since we only focus on the US here
    # later expand world wide
    df_demo = df_demo.dropna(subset=['port_state'])
    df_demo = df_demo.join(demographics, (df_demo.port_city == demographics.City) & (df_demo.port_state_short == demographics['State Code']))
    columns_to_drop = ['port_city', 'port_state', 'port_state_short']
    df_demo = df_demo.drop(*columns_to_drop)

    # reformat column names
    df_demo = rename_columns(df_demo)
    df_demo.write.parquet(os.path.join(outputfolder, 'city_demographics.parquet'), 'overwrite')


def spark_read_csv(folder, filename, **kwargs):

    return spark.read.format('csv').options(header='true', inferSchema=True, **kwargs).\
        load(os.path.join(folder, filename))

def create_country_table(datafolder, outputfolder):
    cntyl = read_csv(datafolder, 'i94cntyl.csv', sep=',', quotechar="'")
    cntyl.write.parquet(os.path.join(outputfolder, 'country.parquet'), 'overwrite')
    return cntyl

def create_full_time_table(outputfolder):
    """
    This creates a full time table until the year 2060. This should reduce the processing
    time a bit, since the dates don't have to processed each time new facts are added
    """
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
    return timeframe


def generate_climate_country(folder, filename, outputfolder):
    climate = spark_read_csv(folder, filename, sep=',')
    climate = climate.withColumn('dt', F.to_date(F.col('dt')))
    country = spark.read.option("mergeSchema", "true").parquet(os.path.join(outputfolder, 'country.parquet'))
    climate = climate.join(country, on=country['i94cntyl'] == climate['Country'], how='leftouter')
    climate = climate.drop('i94cntyl')
    climate = rename_columns(climate)
    climate = climate.withColumn('year', F.year('dt').alias('year'))
    climate = climate.withColumnRenamed('id', 'country_id').\
        withColumnRenamed('averagetemperatureuncertainty', 'avg_uncertainty').\
        withColumnRenamed('averagetemperature', 'avg_temperature')
    climate.write.partitionBy('year', 'country').parquet(os.path.join(outputfolder, 'climate_country.parquet'), 'overwrite')
    return climate

def create_annual_temp_table(outputfolder):
    annual = spark.read.option("mergeSchema", "true").parquet(os.path.join(OUTPUTFOLDER, 'climate_country.parquet'))
    annual.groupby([F.col('country'), F.col('country_id'), F.col('year')]).agg(F.avg('avg_temperature').alias('avg_temperature'))
    annual.write.partitionBy('country').parquet(os.path.join(outputfolder, 'annual_climate_country.parquet'), 'overwrite')
    return annual