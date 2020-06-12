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

The immigration table is the fact table, which should be updated monthly in the future. It contains links to the dimension tables. The tables `transport`, `visa` and `country` are just lookup tables for the identifiers, which are stored in the fact table. The `city_demographics` table is linked via the `prtl_city_id` and contains information on the airport cities. The `dates` table links the i94 date codes to the actual date as well as day, weekday and zear

The temperature data for countries was being averaged by year, so that it is easy for analysts to make an assumption, which countries have been especially affected by climate change by comparing it to the global trend in temperature rise.


## ETL processes Overview

Sustainify was in a hurry when they layed out the architecture for the project, because climate change is a pressing issue. So the first first prototype was geared towards being a quick but robust result. Once the first tests have been made, the whole pipeline will be more automated.

### PySpark ETL Scripts

### `etl_dimension.py`

The ETL script for the dimension data generates all the dimension and lookup tables. For some it is as simple as loading a csv file and storing it directly as a parquet file.
The `dates` table was completely being generated. Since the datecodes in the `arrdate` and `depdate` are modeled in "days since 1960" it is easy to generate a mapping table for enough dates in the future and avoids writing complicated code, which has to be ran continuously.

The temperature data is generated by renaming and selecting the important columns. For the table `temperature_annual_country` and aggregation has to be made, averaging the temperatures for each country by year.

The demographics table is generated by mapping the city and state short sign and joining them with the demographics table. This way the demographics contains a foreign key to the immigation dataset.



- how to run them


### Loading Data to Redshift with Airflow Pipelines


- Description
- how to run on Udacity environment
- Desciption of the DAG

#### Data Quality Checks

Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:

Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing
Source/Count checks to ensure completeness
Run Quality Checks



### Data Cleaning
Cleaning Steps
Document steps necessary to clean the data

Since the 'race' column in the demographics dataset was obmitted. All extra entries for the race count had to be omitted. This was simply done by selecting only the needed columns and dropping the duplicates.

### Data Pipelines

3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model

- how often to update data and why






## Data Dictionary
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.





## Future Outlook
### Scaling up the Platform

Write a description of how you would approach the problem differently under the following scenarios:
The data was increased by 100x.
- use partition by on dimension and fact tables
The data populates a dashboard that must be updated on a daily basis by 7am every day.
The database needed to be accessed by 100+ people.

###  Additional Datasets
The analysis database will be expanded in the future, but as of the deadline, additional datasets could unfortunately not be implemented yet. One interesting dataset to add would be the average inflation-adjusted price of inbound flights. Analysing this data could give valuable insights on whether prices have incentivised people to travle and whether prices can be used to reduce the volume of travels.

Another interesting dataset to include would be the number of asylum seekers to the US. By combining the numbers with the average temperatures of the countries of origin over the years you might be able to give a hint on refugees for climate change reasons.

