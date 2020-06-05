class SqlQueries:

    
    drop_tables = ("""
        DROP TABLE IF EXISTS public.transport;
        DROP TABLE IF EXISTS public.dates;
       
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
    
    """)
    
    