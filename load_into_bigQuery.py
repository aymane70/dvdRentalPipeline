from google.cloud import bigquery
from google.cloud import storage
from dotenv import load_dotenv
import os

# Load the .env file
load_dotenv()


credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

project = os.getenv("GCP_PROJECT")
dataset = os.getenv("BIGQUERY_DATASET")
bucket_name = os.getenv("BUCKET_NAME")
prefix = "raw/"   # folder in bucket


bq = bigquery.Client()
gcs = storage.Client()

bucket = gcs.bucket(bucket_name)

try : 
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name.endswith(".csv"):
            table_name = blob.name.split("/")[-1].replace(".csv", "")
            table_id = f"{project}.{dataset}.{table_name}"

            print(f"Loading {blob.name} → {table_id}")

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                autodetect=True,
                skip_leading_rows=1 ,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )

            load_job = bq.load_table_from_uri(
                f"gs://{bucket_name}/{blob.name}",
                table_id,
                job_config=job_config
            )
            load_job.result()
            print("✔ Loaded", table_id)

except Exception as e:

    print("Error:", e)