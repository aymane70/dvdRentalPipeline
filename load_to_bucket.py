from google.cloud import storage
from dotenv import load_dotenv
import os


load_dotenv()


credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = storage.Client()
bucket_name = os.getenv("BUCKET_NAME")
bucket = client.bucket(bucket_name)


tables = os.getenv("TABLES").split(",")

try :
    for table in tables:
        local_file = f"extracted_tables/{table}_extract.csv"
        destination_blob_name = f"raw/{table}_extract.csv"
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(local_file)
        
        print(f"{local_file} uploaded to gs://{bucket_name}/{destination_blob_name}")

except Exception as e:
    print("Error:", e)
