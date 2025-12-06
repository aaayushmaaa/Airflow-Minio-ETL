# dags/extract_dag.py
import os
import io
import gzip
import logging
import boto3
from botocore.client import Config
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

GDRIVE_CRED_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")
PROCESSED_FILE_PATH = os.environ.get("PROCESSED_FILE_PATH")

logging.basicConfig(level=logging.INFO)


def get_drive_service():
    logging.info("Initializing Google Drive service...")
    creds = service_account.Credentials.from_service_account_file(
        GDRIVE_CRED_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def extract_file(file):
    logging.info(f"Processing file: {file['name']} (ID: {file['id']})")

    # Download file from Drive
    try:
        service = get_drive_service()
        request = service.files().get_media(fileId=file["id"])
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)
        raw_bytes = fh.read()
        raw_size_bytes = len(raw_bytes)
        logging.info(
            f"Downloaded {file['name']} from Drive successfully. bytes={raw_size_bytes}")
    except Exception as e:
        logging.error(f"Failed to download {file['name']} from Drive: {e}")
        raise

    # Upload to MinIO and create compressed bytes
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        # Create bucket if not exists
        if MINIO_BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
            s3.create_bucket(Bucket=MINIO_BUCKET)
            logging.info(f"Created bucket: {MINIO_BUCKET}")

        raw_key = f"raw/{file['name']}"
        gz_key = f"compressed/{file['name']}.gz"

        # Upload raw
        s3.put_object(Bucket=MINIO_BUCKET, Key=raw_key, Body=raw_bytes)
        logging.info(f"Uploaded raw file to MinIO: {raw_key}")

        # Create gz in memory
        gz_buf = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buf, mode="wb") as gz:
            gz.write(raw_bytes)
        gz_buf.seek(0)
        compressed_bytes = gz_buf.read()
        compressed_size_bytes = len(compressed_bytes)

        # Upload compressed
        s3.put_object(Bucket=MINIO_BUCKET, Key=gz_key, Body=compressed_bytes)
        logging.info(f"Uploaded compressed file to MinIO: {gz_key}")

        # Generate presigned URL (valid for 1 hour)
        try:
            presigned = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": MINIO_BUCKET, "Key": gz_key},
                ExpiresIn=3600
            )
        except Exception as e:
            logging.warning(f"Failed to generate presigned URL: {e}")
            presigned = None

        # Save compressed file locally for attaching to email
        local_tmp_dir = "/tmp/airflow_attachments"
        os.makedirs(local_tmp_dir, exist_ok=True)
        local_gz_path = os.path.join(local_tmp_dir, f"{file['id']}.gz")
        with open(local_gz_path, "wb") as f_local:
            f_local.write(compressed_bytes)
        logging.info(
            f"Wrote local compressed file for attachment: {local_gz_path}")

    except Exception as e:
        logging.error(f"Failed to upload {file['name']} to MinIO: {e}")
        raise

    # Mark as processed
    try:
        os.makedirs(os.path.dirname(PROCESSED_FILE_PATH), exist_ok=True)
        with open(PROCESSED_FILE_PATH, "a") as f:
            f.write(file["id"] + "\n")
        logging.info(f"Marked {file['name']} as processed.")
    except Exception as e:
        logging.error(f"Failed to mark {file['name']} as processed: {e}")

    return {
        "file_name": file["name"],
        "file_id": file["id"],
        "raw_size_bytes": raw_size_bytes,
        "compressed_size_bytes": compressed_size_bytes,
        "raw_path": f"s3a://{MINIO_BUCKET}/{raw_key}",
        "compressed_path": f"s3a://{MINIO_BUCKET}/{gz_key}",
        "compressed_presigned_url": presigned,
        "compressed_local_path": local_gz_path
    }
