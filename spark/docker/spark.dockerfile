FROM openjdk:11 as apache-base

RUN apt-get update && \
    apt-get install -y curl \
    vim \
    wget \
#    scala \
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

ENV SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    SCALA_VERSION=2.13 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz" \
    && wget https://mirrors.estointernet.in/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz \
    && tar -xvf apache-maven-3.6.3-bin.tar.gz \
    && mv apache-maven-3.6.3 /opt/ \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

ENV M2_HOME='/opt/apache-maven-3.6.3' \
    PATH="/opt/apache-maven-3.6.3/bin:$PATH"


# Apache spark with Iceberger environment
FROM apache-base as apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs

ENV SPARK_MASTER_LOG=$SPARK_LOG_DIR/spark-master.out \
    SPARK_WORKER_LOG=$SPARK_LOG_DIR/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

EXPOSE 8080 7077 6066 10000

RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

COPY start-spark-thrift.sh /

COPY start-pyspark.sh /

RUN chmod +x /start-spark.sh
RUN chmod +x /start-spark-thrift.sh
RUN chmod +x /start-pyspark.sh

CMD ["/bin/bash", "/start-spark.sh"]


FROM apache-spark as spark-sql-iceberg

WORKDIR /opt/spark

ENV PATH=$PATH:/opt/spark/bin:/opt/spark/sbin

##SPARK-SQL will work at minio simulating AWS S3
ENV DEPENDENCIES="org.postgresql:postgresql:42.6.0\
,org.apache.iceberg:iceberg-bundled-guava:1.3.0\
,org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.3.0\
,org.apache.iceberg:iceberg-hive-runtime:1.3.0\
,org.apache.iceberg:iceberg-hive-metastore:1.3.0\
,org.slf4j:slf4j-simple:2.0.7"\
    AWS_SDK_VERSION=2.20.18 \
    AWS_MAVEN_GROUP=software.amazon.awssdk \
    AWS_ACCESS_KEY_ID=admin \
    AWS_SECRET_ACCESS_KEY=Eqcu3%#Gq6NV \
    AWS_REGION=us-east-1 \
    IP=127.0.0.1 \
    PORT=10000

RUN mkdir -p /workspace/spark-events | true

COPY start-spark-sql.sh /
COPY run-maven.sh /

RUN chmod +x /run-maven.sh
RUN chmod +x /start-spark-sql.sh

RUN /run-maven.sh
