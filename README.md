# Airflow ETL Pipeline — Google Drive → MinIO → PySpark → Email Alerts

This project implements a **complete ETL pipeline** that automates ingestion, transformation, storage, and notification of CSV files from Google Drive. It uses:

- **Apache Airflow** for orchestration
- **MinIO** as S3-compatible storage
- **PySpark** for transformation and analytics
- **Google Drive API** for file ingestion
- **SMTP** email for notifications
- **Docker & Docker Compose** for containerized deployment

---

## High-Level Pipeline Overview

The pipeline automates the following workflow:

1. **Watch Google Drive**
   - Monitors a configured folder for new `.csv` files
   - Skips already processed files
   - Returns new files to Airflow via XCom

2. **Extract & Upload (`extract_file`)**
   - Downloads new files from Google Drive
   - Uploads raw files to MinIO (`raw/` folder)
   - Compresses files to `.gz` and uploads to MinIO (`compressed/` folder)
   - Saves local copies for email attachments
   - Marks files as processed

3. **Transform (`transform_file`)**
   - Reads compressed files from MinIO using PySpark
   - Computes top 10 values/counts of the first column (numeric or categorical)
   - Generates an HTML table for email preview
   - No new files are written; mainly used for analytics preview

4. **Email Notification (`send_upload_email`)**
   - Sends a summary email containing:
     - File names & IDs
     - Raw and compressed file sizes
     - Top 10 records preview (HTML table)
     - Links to download compressed files (via presigned URLs)
   - Attaches local compressed files for convenience

---

## DAG Flow

```text
watch_dag (detect new CSVs)
        |
        v
extract_file (download + upload raw & compressed)
        |
        v
transform_file (compute top 10 preview)
        |
        v
send_email (summary & attachments)
```
All tasks are executed sequentially in the `master_drive_pipeline` DAG:

```text
watch_dag (detect new CSVs)
        |
        v
extract_file (download + upload raw & compressed)
        |
        v
transform_file (compute top 10 preview)
        |
        v
send_email (summary & attachments)
```
**No separate load DAG** is required; uploading to MinIO is handled within `extract_file`.  
Transformation (`transform_file`) is mainly for **analytics preview and email reporting**, not for creating new files on MinIO.

---

## Features

- ✅ Fully automated ETL pipeline
- ✅ Google Drive → MinIO → PySpark → Email alerts
- ✅ Detects new files only (prevents re-processing)
- ✅ Compresses files and generates presigned URLs
- ✅ HTML preview of top 10 records
- ✅ Sends email notifications with attachments
- ✅ Optimized for Mac M1/M2 and Linux environments
- ✅ Modular and containerized using Docker

---

## Folder Structure

```text
project/
│── dags/
│   ├── master_pipeline_dag.py      # Orchestrates all tasks
│   ├── watch_drive_dag.py          # Detects new files in Google Drive
│   ├── extract_dag.py              # Downloads & uploads files to MinIO
│   ├── transform_dag.py            # PySpark transformation & top 10 preview
│── processed_files/                 # Stores processed file IDs
│── logs/                             # Airflow logs
│── gdrive/                           # Google service account JSON
│── Dockerfile
│── docker-compose.yml
│── .env
│── README.md
```
## Environment Variables (.env)

```text
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
SOURCE_DRIVE_FOLDER_ID=<your_drive_folder_id>
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=<minio_access_key>
MINIO_SECRET_KEY=<minio_secret_key>
MINIO_BUCKET=<bucket_name>
PROCESSED_FILE_PATH=/opt/airflow/processed_files/processed_files.txt
EMAIL_TO=your-email@example.com
```
## MinIO Configuration

- URL: `http://localhost:9000`
- Buckets created automatically:
  - `raw` → Stores raw CSVs
  - `compressed` → Stores `.gz` compressed files
- Files can be accessed via **presigned URLs** (valid for 1 hour)

---

## PySpark Configuration

- Spark session configured to access MinIO via Hadoop S3A:
  - `fs.s3a.endpoint`
  - `fs.s3a.access.key`
  - `fs.s3a.secret.key`
  - `fs.s3a.path.style.access = true`
  - `fs.s3a.connection.ssl.enabled = false`
- Reads the first column of each file:
  - Numeric → top 10 values
  - Categorical → top 10 frequent categories
- Generates HTML preview for email reports

---

## Running the Pipeline

1. **Start Services**

```bash
docker-compose up --build -d
```
2. **Access Airflow**

- Web UI: [http://localhost:8080](http://localhost:8080)
- Credentials: configured in `.env` or `docker-compose.yml`

3. **Upload Files**

- Place CSV files in the monitored Google Drive folder
- DAG runs every minute (`*/1 * * * *`) or can be triggered manually

4. **Monitor Execution**

- Airflow logs show task progress
- MinIO contains `raw/` and `compressed/` files
- Emails contain top 10 preview and download links

---

## Notes

- No separate load DAG is required; MinIO upload happens in `extract_file`.
- `transform_file` currently does **not write back cleaned files** to MinIO—used only for preview.
- Processed file IDs are tracked in `processed_files/processed_files.txt`.

---

## Conclusion

This project provides a **production-ready ETL workflow** using:

- **Airflow** → orchestration  
- **PySpark** → scalable transformation  
- **MinIO** → object storage  
- **Google Drive** → source system  
- **SMTP Email** → notifications
