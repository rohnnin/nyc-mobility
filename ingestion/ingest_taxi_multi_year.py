from google.cloud import storage
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# CONFIG
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
BUCKET_NAME = "nyc-mobility-lakehouse"

YEARS = [2022, 2023, 2024]
MONTHS = range(1, 13)

MAX_WORKERS = 4  # control parallelism


client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


def process_file(year, month):
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = BASE_URL + file_name

    blob_path = f"raw/taxi/year={year}/month={month:02d}/{file_name}"
    blob = bucket.blob(blob_path)

    try:
        # ✅ Idempotency check
        if blob.exists():
            return f"SKIPPED: {file_name}"

        # Download
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            return f"FAILED DOWNLOAD: {file_name}"

        with open(file_name, "wb") as f:
            f.write(response.content)

        # Upload
        blob.upload_from_filename(file_name)

        # Cleanup
        os.remove(file_name)

        return f"SUCCESS: {file_name}"

    except Exception as e:
        return f"ERROR: {file_name} | {str(e)}"


def main():
    tasks = [(y, m) for y in YEARS for m in MONTHS]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_file, y, m) for y, m in tasks]

        for future in tqdm(as_completed(futures), total=len(futures)):
            print(future.result())


if __name__ == "__main__":
    main()