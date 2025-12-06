# dags/master_pipeline_dag.py
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp

from watch_drive_dag import find_new_csvs
from extract_dag import extract_file
from transform_dag import transform_file

default_args = {"owner": "airflow"}


def extract_file_wrapper(**context):
    files = context['ti'].xcom_pull(task_ids='watch_dag')
    if not files:
        return []
    results = []
    for file in files:
        results.append(extract_file(file))
    return results


def transform_file_wrapper(**context):
    extracted_files = context['ti'].xcom_pull(task_ids='extract_file')
    if not extracted_files:
        return []
    results = []
    for f in extracted_files:
        results.append(transform_file(f))
    return results


def human_readable_size(n):
    # simple formatter
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024.0:
            return f"{n:0.1f} {unit}"
        n /= 1024.0
    return f"{n:0.1f} PB"


def send_upload_email(**context):
    files = context['ti'].xcom_pull(task_ids='transform_file')
    if not files:
        # No new files, don't send email
        return

    # Collect attachments
    attachments = []

    body = """
    <div style="font-family: Arial, sans-serif; padding: 20px; background-color: #f6fff6;">
        <h2 style="color:#2d6a4f; text-align:center;">Google Drive File Upload – Summary</h2>
        <p>Hello,</p>
        <p>The following file(s) were uploaded to Google Drive and processed successfully.</p>
    """

    # files is a list of dicts produced by transform_file(), which itself received extract_file() output
    for f in files:
        # sizes may be present in the dict returned from extract_file()
        raw_size_hr = human_readable_size(f.get("raw_size_bytes", 0))
        compressed_size_hr = human_readable_size(
            f.get("compressed_size_bytes", 0))
        presigned = f.get("compressed_presigned_url")
        attachment_local = f.get("compressed_local_path")

        # attach if local file exists
        if attachment_local and os.path.exists(attachment_local):
            attachments.append(attachment_local)

        # clickable link: prefer presigned URL, fallback to showing compressed filename text
        download_link_html = (
            f"<a href='{presigned}' style='color:#1b4332; text-decoration: none;'>Download .gz</a>"
            if presigned else
            f"{f.get('compressed_path', 'compressed file')}"
        )

        body += f"""
        <div style="background:#ffffff; padding:15px; margin-bottom:20px; border-radius:8px; border:1px solid #d4f1d4;">
            <h3 style="color:#2b9348; margin-bottom:8px;">{f.get('file_name', 'Unnamed')}</h3>

            <!-- Details Table (shows sizes, not paths) -->
            <table style="width:100%; border-collapse: collapse; margin-bottom: 12px;">
                <tr style="background:#d8f3dc;">
                    <th style="padding:10px; text-align:left; border:1px solid #b7e4c7;">Field</th>
                    <th style="padding:10px; text-align:left; border:1px solid #b7e4c7;">Value</th>
                </tr>
                <tr>
                    <td style="padding:10px; border:1px solid #b7e4c7;">File ID</td>
                    <td style="padding:10px; border:1px solid #b7e4c7;">{f.get('file_id', 'N/A')}</td>
                </tr>
                <tr>
                    <td style="padding:10px; border:1px solid #b7e4c7;">Raw Size</td>
                    <td style="padding:10px; border:1px solid #b7e4c7;">{raw_size_hr}</td>
                </tr>
                <tr>
                    <td style="padding:10px; border:1px solid #b7e4c7;">Compressed Size</td>
                    <td style="padding:10px; border:1px solid #b7e4c7;">{compressed_size_hr}</td>
                </tr>
            </table>

            <!-- Top 10 Table -->
            <h4 style="color:#2d6a4f; margin-top:10px;">Top 10 Records</h4>
            {f.get('html', '<p>No preview available</p>')}
        </div>
        """

    body += "<p style='color:#2d6a4f; font-weight:600;'>Airflow Automation</p></div>"

    # send email and attach compressed files (if any)
    send_email_smtp(
        to=os.environ.get("EMAIL_TO"),
        subject="New Google Drive Files Processed",
        html_content=body,
        files=attachments if attachments else None
    )


with DAG(
    dag_id="master_drive_pipeline",
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["minio", "spark"]
) as dag:

    t1 = PythonOperator(
        task_id="watch_dag",
        python_callable=find_new_csvs
    )

    t2 = PythonOperator(
        task_id="extract_file",
        python_callable=extract_file_wrapper
    )

    t3 = PythonOperator(
        task_id="transform_file",
        python_callable=transform_file_wrapper
    )

    t4 = PythonOperator(
        task_id="send_email",
        python_callable=send_upload_email
    )

    t1 >> t2 >> t3 >> t4
