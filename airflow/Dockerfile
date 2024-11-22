FROM apache/airflow:2.10.2
USER root


RUN apt-get update \
    && apt-get install -y software-properties-common \
    curl \
    vim \
    wget


# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME  /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

ENV DEPENDENCIES="org.postgresql:postgresql:42.6.0\
,org.apache.iceberg:iceberg-bundled-guava:1.5.0\
,org.apache.iceberg:iceberg-core:1.5.0\
,org.apache.iceberg:iceberg-aws:1.5.0\
,org.apache.iceberg:iceberg-spark:1.5.0\
,org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.5.0\
,org.apache.iceberg:iceberg-spark-extensions-3.4_2.13:1.5.0\
,org.apache.iceberg:iceberg-hive-runtime:1.5.0\
,org.apache.iceberg:iceberg-hive-metastore:1.5.0\
,org.slf4j:slf4j-simple:2.0.7"\
    AWS_SDK_VERSION=2.20.120 \
    AWS_MAVEN_GROUP=software.amazon.awssdk \
    IP=127.0.0.1 \
    PORT=10000


ENV SPARK_VERSION=3.4.0 \
    HADOOP_VERSION=3 \
    SCALA_VERSION=2.13 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1


RUN wget -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}-scala${SCALA_VERSION}.tgz"
RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
RUN tar -xvf apache-maven-3.9.9-bin.tar.gz
RUN mv apache-maven-3.9.9 /opt/
RUN mkdir -p /opt/spark
RUN tar -xf apache-spark.tgz -C /opt/spark --strip-components=1
RUN rm apache-spark.tgz

RUN echo "export SPARK_LOCAL_IP=18.220.89.149" >> $SPARK_HOME/conf/spark-env.sh

COPY run-maven.sh /

ENV M2_HOME='/opt/apache-maven-3.9.9' \
    PATH="/opt/apache-maven-3.9.9/bin:$PATH" \
    SPARK_MASTER="spark://spark-master:7077"

RUN ["/bin/bash", "/run-maven.sh"]


USER airflow

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

COPY --chown=airflow:root ./dags /opt/airflow/dags


