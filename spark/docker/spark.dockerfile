FROM docker.io/bitnami/spark:3

USER root

ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN apt-get update && apt-get install -y wget && \
    cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.230/bundle-2.17.230.jar && \
    wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/0.13.2/iceberg-spark-runtime-3.2_2.12-0.13.2.jar

COPY start-spark-sql.sh /opt/bitnami/spark/sbin/start-spark-sql.sh
RUN chmod +x /opt/bitnami/spark/sbin/start-spark-sql.sh

RUN apt-get -y install gnupg2 && \
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update &&\
    apt-get -y install postgresql-client

#Bitnami SPARK Config
ENV SPARK_MODE=master
ENV SPARK_RPC_AUTHENTICATION_ENABLED=no
ENV SPARK_RPC_ENCRYPTION_ENABLED=no
ENV SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
ENV SPARK_SSL_ENABLED=no

#SPARK-SQL will work at minio simulating AWS S3
ENV AWS_ACCESS_KEY_ID=admin
ENV AWS_SECRET_ACCESS_KEY=Eqcu3%#Gq6NV
ENV AWS_S3_ENDPOINT=localhost:9000
ENV AWS_REGION=us-east-1
ENV MINIO_REGION=us-east-1
ENV DEPENDENCIES="org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2"
ENV AWS_SDK_VERSION=2.17.230
ENV AWS_MAVEN_GROUP=software.amazon.awssdk
