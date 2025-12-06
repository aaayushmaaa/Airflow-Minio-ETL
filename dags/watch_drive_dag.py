# dags/watch_drive_dag.py
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

GDRIVE_CRED_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
SOURCE_DRIVE_FOLDER_ID = os.environ.get("SOURCE_DRIVE_FOLDER_ID")
PROCESSED_FILE_PATH = os.environ.get(
    "PROCESSED_FILE_PATH", "/opt/airflow/processed_files/processed_files.txt"
)


def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        GDRIVE_CRED_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def find_new_csvs():
    service = get_drive_service()
    q = f"'{SOURCE_DRIVE_FOLDER_ID}' in parents and trashed=false and (mimeType='text/csv' or name contains '.csv')"
    resp = service.files().list(
        q=q, fields="files(id, name, mimeType, size, createdTime)"
    ).execute()
    files = resp.get("files", [])

    os.makedirs(os.path.dirname(PROCESSED_FILE_PATH), exist_ok=True)
    try:
        with open(PROCESSED_FILE_PATH) as f:
            processed = set(f.read().splitlines())
    except FileNotFoundError:
        processed = set()

    new_files = [f for f in files if f["id"] not in processed]
    return new_files
