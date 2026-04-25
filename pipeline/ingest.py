import requests
import os

DATA_FOLDER = "data"

FILES = {
    "fhvhv_tripdata_2023-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2023-01.parquet",
    "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
}

def download_file(filename, url):
    destination = os.path.join(DATA_FOLDER, filename)

    # Don't re-download if it already exists
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
    for filename, url in FILES.items():
        download_file(filename, url)
    print("All files downloaded.")