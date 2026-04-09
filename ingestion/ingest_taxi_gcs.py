from google.cloud import storage
import requests
import os

client = storage.Client()
bucket = client.bucket("nyc-mobility-lakehouse")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

year = 2023
month = 12

file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
url = BASE_URL + file_name

blob_path = f"raw/taxi/year={year}/month={month:02d}/{file_name}"
blob = bucket.blob(blob_path)

if blob.exists():
    print("Already exists, skipping")
else:
    print("Downloading...")
    response = requests.get(url)

    with open(file_name, "wb") as f:
        f.write(response.content)

    print("Uploading to GCS...")
    blob.upload_from_filename(file_name)

    os.remove(file_name)