
#!/bin/bash

export AWS_PACKAGES=(
  "bundle"
  "url-connection-client"
)

for pkg in "${AWS_PACKAGES[@]}"; do
    export DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

export S3IP=$(cat /etc/hosts | grep host.docker.internal | awk '{print $1}')

spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=jdbc:postgresql://host.docker.internal:5420/db_iceberg \
    --conf spark.sql.catalog.bios.jdbc.user=icbergcat \
    --conf spark.sql.catalog.bios.jdbc.password=hNXz35UBRcAC \
    --conf spark.sql.catalog.bios.warehouse=s3a://bios/ \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=http://${S3IP}:9000 \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.defaultCatalog=bios \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/workspace/spark-events \
    --conf spark.history.fs.logDirectory= /workspace/spark-events \
    --conf spark.sql.catalogImplementation=in-memory

#   CREATE TABLE bios.table_01 (
#          id bigint,
#          data string,
#          category string)
#   USING iceberg
#   PARTITIONED BY (category);



#CREATE TABLE bios.tabela_particionada01 (id bigint, data string, category string) USING iceberg PARTITIONED BY (category);
#INSERT INTO bios.tabela_particionada01 VALUES (1, 'Testando meus dados', 'teste');
