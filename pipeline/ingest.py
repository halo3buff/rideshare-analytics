import requests
import os

DATA_FOLDER = "data"

# Generate all 12 months of 2023 trip files
MONTHS = [f"2023-{str(m).zfill(2)}" for m in range(1, 13)]

FILES = {
    f"fhvhv_tripdata_{month}.parquet": 
    f"https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{month}.parquet"
    for month in MONTHS
}

# Add zones file
FILES["taxi_zone_lookup.csv"] = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

def download_file(filename, url):
    destination = os.path.join(DATA_FOLDER, filename)

    if os.path.exists(destination):
        print(f"{filename} already exists, skipping.")
        return
    
    print(f"Downloading {filename}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(destination, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Saved to {destination}")

if __name__ == "__main__":
    os.makedirs(DATA_FOLDER, exist_ok=True)
    for filename, url in FILES.items():
        download_file(filename, url)
    print("All files downloaded.")