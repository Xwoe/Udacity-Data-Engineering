class SqlQueries:

    
    drop_tables = ("""
        DROP TABLE IF EXISTS public.transport_mode;
        DROP TABLE IF EXISTS public.dt_time;
       
    """)
    
    create_tables = ("""
        CREATE TABLE IF NOT EXISTS public.transport_mode (
            model_id int4 NOT NULL,
            model varchar(20)
                );
        
        CREATE TABLE IF NOT EXISTS public.dt_time (
            i_date int, 
            dt_date date NOT NULL,
            "year" int4,
            "month" int4,
            day_of_month int4,
            weekday int4,
            CONSTRAINT dt_time_pkey PRIMARY KEY (i_date)
            );
    
    """)
    
    