# dags/transform_dag.py
import os
from pyspark.sql import SparkSession
from airflow.utils.email import send_email_smtp

# Environment variables
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")
EMAIL_TO = os.environ.get("EMAIL_TO")


def get_spark_session():
    """
    Creates a SparkSession with Hadoop AWS jars configured.
    Stops any existing session to ensure jars are loaded.
    """
    try:
        spark = SparkSession.builder.getOrCreate()
        spark.stop()  # Stop default session to recreate with jars
    except Exception:
        pass

    spark = SparkSession.builder \
        .appName("top10_transform") \
        .config(
            "spark.jars",
            "/opt/spark/jars/hadoop-common-3.3.6.jar,"
            "/opt/spark/jars/hadoop-aws-3.3.6.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.540.jar"
        )\
        .getOrCreate()

    # Hadoop/S3A configuration for MinIO
    hconf = spark._jsc.hadoopConfiguration()
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    hconf.set("fs.s3a.endpoint", endpoint)
    hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")

    return spark


def transform_file(file):
    """
    Reads a CSV file from MinIO, computes top 10 values/counts, and returns HTML table.
    """
    if not file:
        return None

    spark = get_spark_session()

    # Read CSV from compressed path in MinIO
    df = spark.read.option("header", "true").csv(file["compressed_path"])

    col = df.columns[0]

    # Numeric column
    if df.schema[col].dataType.simpleString() in ["int", "double", "float", "long"]:
        top = df.select(col).orderBy(df[col].desc()).limit(10).toPandas()
        html = "<table border='1'><tr><th>Value</th></tr>"
        for v in top[col].values:
            html += f"<tr><td>{v}</td></tr>"
        html += "</table>"

    # Categorical column
    else:
        top = df.groupBy(col).count().orderBy(
            "count", ascending=False).limit(10).toPandas()
        html = "<table border='1'><tr><th>Value</th><th>Count</th></tr>"
        for _, row in top.iterrows():
            html += f"<tr><td>{row[col]}</td><td>{row['count']}</td></tr>"
        html += "</table>"

    return {**file, "html": html}
