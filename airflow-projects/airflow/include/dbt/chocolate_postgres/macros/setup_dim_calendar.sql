{% macro setup_dim_calendar() %}

    {% set sql_statement %}
        -- 1. Create the dedicated DWH schema if it doesn't exist
        CREATE SCHEMA IF NOT EXISTS dwh;

        -- 2. Create the table explicitly inside the 'dwh' schema
        CREATE TABLE IF NOT EXISTS dwh.dwh_dim_calendar (
            Date_ID INT PRIMARY KEY,
            Full_Date DATE NOT NULL,
            Year SMALLINT NOT NULL,
            Quarter SMALLINT NOT NULL,
            Month SMALLINT NOT NULL,
            MonthName VARCHAR(30) NOT NULL,
            Week SMALLINT NOT NULL,
            Day_Of_Week SMALLINT NOT NULL,
            Day_Name VARCHAR(15) NOT NULL,
            Is_Weekend BOOLEAN NOT NULL,
            Is_Holiday BOOLEAN NOT NULL DEFAULT FALSE,
            Season VARCHAR(30) NOT NULL
        );

        -- 3. Generate and Insert the data
        INSERT INTO dwh.dwh_dim_calendar
        SELECT
            to_char(datum, 'YYYYMMDD')::INT AS Date_ID,
            datum::DATE AS Full_Date,
            EXTRACT(YEAR FROM datum)::SMALLINT AS Year,
            EXTRACT(QUARTER FROM datum)::SMALLINT AS Quarter,
            EXTRACT(MONTH FROM datum)::SMALLINT AS Month,
            TRIM(to_char(datum, 'Month'))::VARCHAR(30) AS MonthName,
            EXTRACT(WEEK FROM datum)::SMALLINT AS Week,
            EXTRACT(ISODOW FROM datum)::SMALLINT AS Day_Of_Week, 
            TRIM(to_char(datum, 'Day'))::VARCHAR(15) AS Day_Name,
            CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS Is_Weekend,
            FALSE AS Is_Holiday, 
            CASE
                WHEN EXTRACT(MONTH FROM datum) IN (12, 1, 2) THEN 'Winter'
                WHEN EXTRACT(MONTH FROM datum) IN (3, 4, 5)  THEN 'Spring'
                WHEN EXTRACT(MONTH FROM datum) IN (6, 7, 8)  THEN 'Summer'
                WHEN EXTRACT(MONTH FROM datum) IN (9, 10, 11) THEN 'Autumn'
            END::VARCHAR(30) AS Season
        FROM generate_series('2023-01-01'::DATE, '2027-12-31'::DATE, '1 day'::interval) AS datum
        
        -- Prevent duplicate rows if you run the script again
        ON CONFLICT (Date_ID) DO NOTHING;
    {% endset %}

    {% do run_query(sql_statement) %}
    {{ log("Calendar Dimension check/creation completed successfully.", info=True) }}

{% endmacro %}