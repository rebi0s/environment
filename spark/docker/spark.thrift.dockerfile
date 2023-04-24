FROM openjdk:11 as apache-base

RUN apt-get update && \
    apt-get install -y curl \
    vim \
    wget \
    scala \
    software-properties-common \
    ssh net-tools \
    ca-certificates \
    python3 \
    python3-pip \
    python3-numpy \
    python3-matplotlib \
    python3-scipy \
    python3-pandas && \
    pip3 install sympy

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

ENV SPARK_VERSION=3.2.3 \
    HADOOP_VERSION=3.2 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

FROM apache-base as thrift-sql-iceberg

WORKDIR /opt/spark

ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

##SPARK-SQL will work at minio simulating AWS S3
ENV DEPENDENCIES="org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.1,org.apache.iceberg:iceberg-hive-runtime:0.13.1,org.apache.logging.log4j:log4j-core:2.11.2" \
    AWS_SDK_VERSION=2.20.35 \
    AWS_MAVEN_GROUP=software.amazon.awssdk \
    AWS_ACCESS_KEY_ID=admin \
    AWS_SECRET_ACCESS_KEY=Eqcu3%#Gq6NV \
    AWS_REGION=us-east-1

RUN mkdir -p /workspace/spark-events | true

EXPOSE 10000

COPY start-thriftserver.sh /

CMD ["/bin/bash", "/start-thriftserver.sh"]
