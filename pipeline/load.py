import duckdb
import os

DATA_FOLDER = "data"
DB_PATH = "warehouse.duckdb"

def load_trips(con):
    parquet_path = os.path.join(DATA_FOLDER, "fhvhv_tripdata_2023-01.parquet").replace("\\", "/")
    print("Loading trips data...")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_trips AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    count = con.execute("SELECT COUNT(*) FROM raw_trips").fetchone()[0]
    print(f"raw_trips loaded: {count:,} rows")

def load_zones(con):
    csv_path = os.path.join(DATA_FOLDER, "taxi_zone_lookup.csv").replace("\\", "/")
    print("Loading zone lookup...")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_zones AS
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    count = con.execute("SELECT COUNT(*) FROM raw_zones").fetchone()[0]
    print(f"raw_zones loaded: {count:,} rows")

if __name__ == "__main__":
    con = duckdb.connect(DB_PATH)
    load_trips(con)
    load_zones(con)
    con.close()
    print("Done. Database saved to warehouse.duckdb")