import snowflake.connector
import pandas as pd
import os
import glob
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

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
    print("Creating raw tables if they dont exist...")
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
    print("Tables ready.")

def load_trips(sf_con):
    # Find all parquet files in data folder
    parquet_files = sorted(glob.glob("data/fhvhv_tripdata_*.parquet"))
    
    if not parquet_files:
        print("No parquet files found in data/ folder.")
        return

    # Truncate once before loading all files
    print("Truncating RAW_TRIPS before reload...")
    sf_con.cursor().execute("TRUNCATE TABLE IF EXISTS RIDESHARE.RAW.RAW_TRIPS")

    for filepath in parquet_files:
        filename = os.path.basename(filepath)
        print(f"Loading {filename}...")

        df = pd.read_parquet(filepath)
        df.columns = [col.upper() for col in df.columns]

        # Convert timestamps to strings for Snowflake compatibility
        timestamp_cols = ["REQUEST_DATETIME", "ON_SCENE_DATETIME",
                          "PICKUP_DATETIME", "DROPOFF_DATETIME"]
        for col in timestamp_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).replace('NaT', None)

        print(f"  {len(df):,} rows — uploading...")

        success, nchunks, nrows, _ = write_pandas(
            conn=sf_con,
            df=df,
            table_name="RAW_TRIPS",
            schema="RAW",
            database="RIDESHARE",
            chunk_size=100000,
            auto_create_table=False
        )
        print(f"  Done. {nrows:,} rows loaded in {nchunks} chunks.")

    print("All trip files loaded.")

def load_zones(sf_con):
    print("Loading zones...")
    sf_con.cursor().execute("TRUNCATE TABLE IF EXISTS RIDESHARE.RAW.RAW_ZONES")

    df = pd.read_csv("data/taxi_zone_lookup.csv")
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