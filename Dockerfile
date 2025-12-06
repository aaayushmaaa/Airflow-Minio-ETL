# Custom Airflow image
FROM apache/airflow:2.10.1-python3.11

USER root

# Install Java (needed for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk wget curl tar gzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Switch to airflow user to install Python dependencies
USER airflow
RUN pip install --no-cache-dir \
    google-api-python-client \
    google-auth-httplib2 \
    google-auth-oauthlib \
    minio \
    pyspark==3.5.1 \
    boto3

# Switch back to root to download matching Spark → Hadoop → AWS JARs
USER root

# Hadoop 3.3.4 + AWS Java SDK 1.12.262 (CORRECT VERSIONS)
RUN mkdir -p /opt/spark/jars && \
    wget -q -O /opt/spark/jars/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    chown -R airflow:50000 /opt/spark/jars

# Set Spark classpath
ENV SPARK_CLASSPATH=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar pyspark-shell"

# Switch back to airflow user
USER airflow

ENV AIRFLOW_HOME=/opt/airflow
