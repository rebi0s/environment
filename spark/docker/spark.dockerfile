FROM docker.io/bitnami/spark:3

USER root

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update && apt-get install -y wget && \
    cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.35/bundle-2.20.35.jar && \
    wget https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.35/url-connection-client-2.20.35.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/0.13.2/iceberg-spark-runtime-3.2_2.12-0.13.2.jar

COPY start-spark-sql.sh /opt/bitnami/spark/sbin/start-spark-sql.sh
RUN chmod +x /opt/bitnami/spark/sbin/start-spark-sql.sh

#Bitnami SPARK Config
ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no

#SPARK-SQL will work at minio simulating AWS S3
ENV ICEBERG_VERSION=1.2.0
ENV DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:$ICEBERG_VERSION"
ENV AWS_SDK_VERSION=2.20.35
ENV AWS_MAVEN_GROUP=software.amazon.awssdk
ENV EXTRA_JARS=/opt/bitnami/spark/jars/bundle-2.20.35.jar,/opt/bitnami/spark/jars/url-connection-client-2.20.35.jar,opt/bitnami/spark/jars/iceberg-spark-runtime-3.2_2.12-0.13.2.jar
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=Eqcu3%#Gq6NV
ENV AWS_S3_ENDPOINT=host.docker.internal:9000
ENV AWS_REGION=us-east-1
ENV MINIO_REGION=us-east-1
