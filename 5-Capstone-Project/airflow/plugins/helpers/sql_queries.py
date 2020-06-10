class SqlQueries:

    drop_tables = ("""
        DROP TABLE IF EXISTS public.transport;
        DROP TABLE IF EXISTS public.dates;
        DROP TABLE IF EXISTS public.country;
        DROP TABLE IF EXISTS public.temperature_annual_country;
        DROP TABLE IF EXISTS public.city_demographics;
        DROP TABLE IF EXISTS public.visa;
        DROP TABLE IF EXISTS public.temperature_global;
    """)

    create_tables = ("""
        CREATE TABLE IF NOT EXISTS public.transport (
            model_id int4 NOT NULL,
            "model" varchar(20));

        CREATE TABLE IF NOT EXISTS public.dates (
          i_date int,
          dt_date date NOT NULL,
          "year" int4,
          "month" int4,
          "day" int4,
          weekday int4,
          CONSTRAINT dt_time_pkey PRIMARY KEY (i_date)
          );

        CREATE TABLE IF NOT EXISTS public.country (
          cntyl_id int,
		      cntyl varchar(max),
          CONSTRAINT country_pkey PRIMARY KEY (cntyl_id)
        );

        CREATE TABLE IF NOT EXISTS public.temperature_annual_country (
          cntyl_id int,
          "year" int4,
		      average_temperature double precision,
		      country varchar(max)
        );


        CREATE TABLE IF NOT EXISTS public.city_demographics (
          prtl_city_id varchar(5),
		      city varchar(max),
  		    median_age float,
  		    male_population int,
  		    female_population int,
		      total_population int,
 		      foreignborn int,
		      average_household_size float,
		      state_code varchar(3),
		      state varchar(max)
        );

        CREATE TABLE IF NOT EXISTS public.visa (
          visa_id int,
          visa varchar(20),
          CONSTRAINT visa_pkey PRIMARY KEY (visa_id)
        );

        CREATE TABLE IF NOT EXISTS public.temperature_global (
          "year" int4,
		      land_average_temperature float,
  		    land_average_temperature_uncertainty float,
          land_max_temperature float,
          land_max_temperature_uncertainty float,
          land_min_temperature float,
          land_min_temperature_uncertainty float,
          land_and_ocean_average_temperature float,
          land_and_ocean_average_temperature_uncertainty float
        );
    """)

    drop_fact_table = ("""
         DROP TABLE IF EXISTS public.{table_name};
    """)

    create_fact_table = ("""
        CREATE TABLE IF NOT EXISTS public.{table_name} (
          cicid int NOT NULL,
          i_yr int4,
          i_mon int4,
          arrdate int,
          depdate int,
          i_cit int,
          i_res int,
          i_port varchar(5),
          i_mode int,
          i_addr varchar(3),
          i_bir int,
          i_visa int,
          visatype varchar(3),
          gender varchar(4),
          airline varchar(4),
          fltno varchar(6),
          length_stay int,
          CONSTRAINT {table_name}_pkey PRIMARY KEY (cicid)
        );
    """)

    insert_fact = (
    """
        INSERT INTO immigration
        SELECT s.* FROM immigration_staging s LEFT JOIN immigration
        ON s.cicid = immigration.cicid
        WHERE immigration.cicid IS NULL;

    """
    )