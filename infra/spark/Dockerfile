# Image Spark của FinObs: apache/spark 3.5.3 + connector Kafka + Iceberg.
# Dùng cho spark-master, spark-worker và các job (bronze-stream...). Build context: gốc repo.
FROM apache/spark:3.5.3

ARG MAVEN=https://repo1.maven.org/maven2
ARG SPARK_VERSION=3.5.3
ARG ICEBERG_VERSION=1.6.1

USER root
# Connector Kafka cho Structured Streaming và các phụ thuộc của nó.
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
# Iceberg: catalog REST + S3FileIO (ghi thẳng MinIO, không cần hadoop-aws).
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/

COPY infra/configs/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY platform/streaming/ /opt/finobs/platform/streaming/
RUN mkdir -p /checkpoints && chown spark /checkpoints

USER spark
