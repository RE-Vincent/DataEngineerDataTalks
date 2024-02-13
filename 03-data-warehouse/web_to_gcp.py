from dotenv import load_dotenv
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
# init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet
# switch out the bucketname
load_dotenv()
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")
GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

        # print(f"{init_url}{service}/{file_name}")

def web_to_gcs_from_parquet(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = f"{i+1:02d}"

        # parquet file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # # download parquet file
        parquet_url = f"{init_url}{file_name}"

        r = requests.get(parquet_url)
        with open(file_name, 'wb') as f:
            f.write(r.content)
        print(f"Local: {file_name}")

        # Sube el archivo Parquet a GCS
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")
        # print(parquet_url)


if __name__ == '__main__':
    web_to_gcs_from_parquet('2022', 'green')
    # print(os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname"))
