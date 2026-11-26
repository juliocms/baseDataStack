FROM spark:3.5.9-python3

USER root

ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG SPARK_VERSION=3.5.9

ADD "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" /opt/spark/jars/
ADD "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" /opt/spark/jars/
ADD "https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/${SPARK_VERSION}/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar" /opt/spark/jars/

RUN chmod 644 \
    "/opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar" \
    "/opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
    "/opt/spark/jars/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar"

COPY entrypoint.sh /opt/app/entrypoint.sh
COPY job_server.py /opt/app/job_server.py
COPY jobs /opt/app/jobs

RUN chmod 755 /opt/app/entrypoint.sh \
    && chown -R spark:spark /opt/app \
    && mkdir -p /opt/spark-events \
    && chown spark:spark /opt/spark-events

USER spark

ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PATH="/opt/spark/bin:${PATH}" \
    LAKE_BUCKET=lakehouse \
    EMULATOR_DIR=/aws/emulator/9297

EXPOSE 8091
ENTRYPOINT ["/opt/app/entrypoint.sh"]
