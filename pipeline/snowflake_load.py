import snowflake.connector
import pandas as pd
import duckdb
from dotenv import load_dotenv
import os

load_dotenv()

SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def create_raw_tables(con):
    print("Creating raw tables in Snowflake...")
    con.cursor().execute("""
        CREATE TABLE IF NOT EXISTS RAW.RAW_TRIPS (
            hvfhs_license_num VARCHAR,
            dispatching_base_num VARCHAR,
            originating_base_num VARCHAR,
            request_datetime TIMESTAMP,
            on_scene_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_miles FLOAT,
            trip_time INTEGER,
            base_passenger_fare FLOAT,
            tolls FLOAT,
            bcf FLOAT,
            sales_tax FLOAT,
            congestion_surcharge FLOAT,
            airport_fee FLOAT,
            tips FLOAT,
            driver_pay FLOAT,
            shared_request_flag VARCHAR,
            shared_match_flag VARCHAR,
            access_a_ride_flag VARCHAR,
            wav_request_flag VARCHAR,
            wav_match_flag VARCHAR
        )
    """)
    con.cursor().execute("""
        CREATE TABLE IF NOT EXISTS RAW.RAW_ZONES (
            LocationID INTEGER,
            Borough VARCHAR,
            Zone VARCHAR,
            service_zone VARCHAR
        )
    """)
    print("Tables created.")

def load_trips(sf_con):
    from snowflake.connector.pandas_tools import write_pandas

    print("Reading trips from DuckDB...")
    duck_con = duckdb.connect("warehouse.duckdb")
    df = duck_con.execute("SELECT * FROM raw_trips").df()
    duck_con.close()

    # Uppercase column names to match Snowflake's convention
    df.columns = [col.upper() for col in df.columns]

    # Convert timestamp columns to strings
    timestamp_cols = ["REQUEST_DATETIME", "ON_SCENE_DATETIME",
                      "PICKUP_DATETIME", "DROPOFF_DATETIME"]
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).replace('NaT', None)

    print(f"Loaded {len(df):,} rows from DuckDB. Uploading to Snowflake...")

    success, nchunks, nrows, _ = write_pandas(
        conn=sf_con,
        df=df,
        table_name="RAW_TRIPS",
        schema="RAW",
        database="RIDESHARE",
        chunk_size=100000,
        auto_create_table=False
    )

    print(f"Upload complete. {nrows:,} rows loaded in {nchunks} chunks.")

def load_zones(sf_con):
    from snowflake.connector.pandas_tools import write_pandas
    print("Loading zones...")
    duck_con = duckdb.connect("warehouse.duckdb")
    df = duck_con.execute("SELECT * FROM raw_zones").df()
    duck_con.close()
    df.columns = [col.upper() for col in df.columns]
    success, nchunks, nrows, _ = write_pandas(
        conn=sf_con,
        df=df,
        table_name="RAW_ZONES",
        schema="RAW",
        database="RIDESHARE",
        auto_create_table=False
    )
    print(f"Zones loaded: {nrows:,} rows")

if __name__ == "__main__":
    sf_con = get_snowflake_connection()
    create_raw_tables(sf_con)
    load_zones(sf_con)
    load_trips(sf_con)
    sf_con.close()
    print("All data loaded into Snowflake.")