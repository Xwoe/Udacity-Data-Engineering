# Udacity Data Engineering - Capstone Project

This is the final project of the Udacity Data Engineering Nanodegree. In this solution the provided datasets were used and a preprocessing pipeline based on PySpark was written to transform the raw data and write it to parquet files. These files where then loaded into a Redshift database in two separate Airflow DAGs.

## Sustainify Introduction

Sustainify is an organization, which wants to communicate different aspects of sustainability and climate change with the help of data analysis as their main purpose. They want to offer sustainability analyses for free, but also offer general analyses based around traveling and their reasons as a paid service in order to finance the free access for sustainability reasons.

On the sustainability track they want to create a dashboard for users, which makes it possible for them to explore different aspects of harm our daily habits impose on our planet. One important aspect is traveling. To start out they were kindly provided by Udacity with datasets on inbound visitors to the US, worldwide temperature data and demographic data of US cities.

On the commercial track they want to offer analyses around how and why people travel. What destinations do they choose, where do the most visitors come from and do they choose a certain time of year, maybe due to the weather in their countries of origin?

The type of analyses, which can be done by combining these datasets are first mainly based around the question why and how people travel. One interesting query might be: how many very short business trips are there to the US and how much CO2 could be avoided if they where held via Web-Meetings instead of doing them in person? Other interesting analyses might be whether the travel volume has increased over the years and whether the travel destinations have shifted.

It is planned to add more datasets for the analyses later on. A little outlook for this is given at the end of this document.


## Detailed Description of the Datasets

The datasets provided by Udacity where:
- I94 Immigration dataset + metadata file for description of columns
- Temperature datasets by Country, City, State and Global
- `us-cities-demographics.csv`: Information on the demographics of various US cities
- `airport-codes_csv.csv`: Airport Code mappings

The Airport Codes dataset was not used, since the information about the airports was not important in our case and data on the port cities could be obtained by linking it to the City Demographics data.

### I94 Immigration Dataset

The dataset which contains the facts is the immigration dataset. It contains one entry for each visitor to the US. In this dataset only legally admitting visitors are contained, no applications for permanent stay or asylum.
The dataset contains data about the date and mode of transport of the visitor as well as the purpose of their stay and their age. Also it contains information about the length of the stay (in case the `depdate` column is not null).

In order to obtain information about the columns `i94cit`, `i94res`, `i94port`, `i94mode` and `i94addr` the necessary metadata was extracted from the metadata file. The data was manually extracted and written to csv files. In case of the `i94port` column the mapped data contained the city and short sign of the state, separated by a comma. This data was split in two csv files to allow for easier mapping. The splitting was done by the comma that was contained and then the city name was properly capitalized so that it could later be mapped to match with the data in the city demographics dataset. All csv files contain a key value format, which allows for easy mapping and efficient reuse of code.


Columns, which where not used where the following, because they either provided no value to the analyses or they where mostly null anyway:
- `insnum`
- `dtaddto`
- `matflag`
- `entdepu`
- `entdepa`
- `entdepd`
- `admnum`
- `dtadfile`
- `occup`
- `biryear`


Some columns do not provide information for analyses yet, but they will offer a link to other datasets that will be linked in the future. Thes columns are for example: `airline`, `fltno`. These columns can later be linked to a dataset for average flight fares to conduct analyses on the effect prices have on the total travel volumne.


### Temperature Datasets

There are temperature datasets provided in different geographical granularity. They are aggregated monthly average temperatures divided either by state, major city, city, country or globally. The global temperature dataset also contains temperatures separated by land and ocean. All datasets also contain a statistical uncertainty for the temperature values, which might also be important for the analysts.

For the first iteration of Sustainify it was decided to only take the global and country datasets and average them by year. This should already provide insights for the global climate change trends by country. Later on after checking back with the analysts, more fine granular aggregations might be added.


### US Cities Demographics

This dataset contains demographic data for US cities. It contains numbers of the population divided by race, gender and total numbers. For the race counts one row exists for each race. The race is not important for our analyses and thus the columns were removed and the separated rows where unified. The other columns where kept, inluding the `Foreign-born` column which might be interersting to see for analysis regarding climate change based asylum.


## Architecture

The whole data pipeline consists of two major steps. First the data is being transformed from its raw format into staging tables in parquet format and stored on S3. In the second step the data is being loaded from the parquet files into a Redshift database with two separate Airflow DAGs. One for the dimension data and one for the fact table. The reason why this two step process was chosen is that having the staging tables on S3 makes it easy to try out combinations with new datasets while still maintaining the production database on Redshift.

The choice for Redshift was made since it makes it easier to open up the data for researchers of whom most are familiar with SQL databases and also it allows for concurrent usage of multiple users.

## Data Model Design

For the data model a star schema was selected with one fact table and multiple dimension tables detailing different static data of the tables like mapping ids to their text description for example. The only tables, which are not strictly dimension tables are the temperature tables, which also contain facts linked to the countries. A more detailed description of all the columns is provided in the [data dictionary](./documentation/DataDictionary.pdf)

The star schema can be seen in this ERM:

![Sustainify ERM](images/ERM.png?raw=true "ERM")

The immigration table is the fact table, which should be updated monthly in the future. It contains links to the dimension tables. The tables `transport`, `visa` and `country` are just lookup tables for the identifiers, which are stored in the fact table. The `city_demographics` table is linked via the `prtl_city_id` and contains information on the airport cities. The `dates` table links the i94 date codes to the actual date as well as day, weekday, month and year to allow for easier aggregations based on time dimensions.

The temperature data for countries was being averaged by year, so that it is easy for analysts to make an assumption, which countries have been especially affected by climate change by comparing it to the global trend in temperature rise.


## ETL processes Overview

Sustainify was in a hurry when they layed out the architecture for the project, because climate change is a pressing issue. So the first first prototype was geared towards being a quick but robust result. Once the first tests have been made, the whole pipeline will be more automated. For now the ETL scripts have to be ran manually and are not integrated into an Airflow pipeline yet.

### PySpark ETL Scripts

In order to run the scripts, an EMR instance has to be spawned. Since the scripts rely on Python 3.x and the machines run on Python 2.7 by default, the following code has to be added to the configuration before spawning the EMR instance:
```
[
  {
     "Classification": "spark-env",
     "Configurations": [
       {
         "Classification": "export",
         "Properties": {
            "PYSPARK_PYTHON": "/usr/bin/python3"
          }
       }
    ]
  }
]
```

Then the instance can be accessed via:
```
ssh -i <path/to/spark-cluster.pem> <instance_name>.compute.amazonaws.com
```

A folder has to be created to store the ETL code and pandas has to be installed:
```
mkdir capstone
sudo python3 -m pip install pandas
```
From the local machine, the script files have to be copied onto the instance. Head to the `./etl` folder of this repository and run:
```
scp -i <path/to/spark-cluster.pem> *.py *.cfg <instance_name>.compute.amazonaws.com:~/capstone
```

Then the scripts from within the EMR instance can be run by executing the following commands one by another:

```
/usr/bin/spark-submit --master yarn ./etl_dimension.py
/usr/bin/spark-submit --master yarn ./etl_fact.py
```

The scripts also contain command line parameters to specify input and output folders, but are configured with standard values to point to the S3 folder.



### `etl_dimension.py`

The ETL script for the dimension data generates all the dimension and lookup tables. For some it is as simple as loading a csv file and storing it directly as a parquet file.
The `dates` table was completely being generated. Since the datecodes in the `arrdate` and `depdate` are modeled in "days since 1960" it is easy to generate a mapping table for enough dates in the future and avoids writing complicated code, which has to be run continuously.

The temperature data is generated by renaming and selecting the important columns. For the table `temperature_annual_country` an aggregation has to be made, averaging the temperatures for each country by year.

The demographics table is generated by mapping the city and state short sign and joining them with the demographics table. This way the demographics contains a foreign key to the immigation dataset.

### `etl_fact.py`

In the ETL script for the fact table `immigration` the raw data is read and the schema is applied to map the columns to the right data types. The columns are renamed to not contain any numbers and an extra column reflecting the length of the stay is added.

## Loading Data to Redshift with Airflow Pipelines

The Airflow DAGS `sustainify_fact_dag.py` and `sustainify_dimension_dag.py` can be run in the Airflow environment of final project of the "Data Pipelines" course.

In order for it to run, first a Variable named `arn` with the IAM role for Redshift aceess as a value has to be added. Then the connections `aws_credentials` and `redshift` have to be added, as described in the Data Pipelines class.

The dimension pipeline has to be run first and then the fact pipeline. The dimension pipeline is setup to run only once for now, because the dimension tables contain mostly static data. However in the future it is expected to update the temperature data more frequently. Then this schedule will have to be adapted and probably a separate DAG for the temperature data would have to be implmented. The schedule for the fact table is set to monthly, because it might be expected that new data comes in on a monthly basis.

![Sustainify Dimension Tables Pipeline](images/sustainify_dimensions_dag.png?raw=true "Dimension")

In this pipeline the tables are first being created if they don't exist and then they are filled by copying the data from S3 to Redshift with the `StageToRedshiftOperator`. Finally for each table the `DataQualityOperator` is used to check if the tables are not empty.


![Sustainify Fact Table Pipeline](images/sustainify_fact_dag.png?raw=true "Fact")
In the pipeline for the fact table the fact table is being created if it does not exist and the staging table is being created. The staging table is needed to update the existing records in the fact table with additional new rows. To perform this, first the data from S3 is being copied onto the staging table and then records, which do not exist on the production table are being added, by performing a SQL-join operation on the `cicid`, which is the unique identifier for records in the fact table. After this is done in the `Insert_Facts` step, the staging table can be dropped again and a quality check will be performed. In the `Quality_no_Duplicates` step a check is performed to make sure no duplicates are being stored in the fact table.


## Future Outlook
### Scaling up the Platform

#### What if the data was increased by 100x?

If the amount of data was increase then first the speed of the ETL pipeline on the Spark cluster would have to be improved by increasing the number of worker nodes. Then also to improve the ability to perform actions in parallel the data store would have to be split into different subsets, so that it can be stored on S3 in separate folders e.g. by day and month.


#### What if the data populates a dashboard that must be updated on a daily basis by 7am every day?
In this case the DAG to update the fact table has to be set to run such that it is finished daily before 7am. So it probably makes sense to set the schedule interval to start at 5AM each day:
`schedule_interval='0 5 * * *'`

#### What if the database needed to be accessed by 100+ people?

Since the database runs on Redshift already, there should be no problem handling more than a hundred users. However with regards to query performance ot should be considered to partition the data on dimension and fact tables to make sure that the most important queries run efficiently.

###  Additional Datasets
The analysis database will be expanded in the future, but as of the deadline, additional datasets could unfortunately not be implemented yet. One interesting dataset to add would be the average inflation-adjusted price of inbound flights. Analysing this data could give valuable insights on whether prices have incentivised people to travle and whether prices can be used to reduce the volume of travels.

Another interesting dataset to include would be the number of asylum seekers to the US. By combining the numbers with the average temperatures of the countries of origin over the years you might be able to give a hint on refugees for climate change reasons.

