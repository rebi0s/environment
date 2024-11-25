
#!/bin/bash

# export AWS_PACKAGES=(
#   "bundle"
#   "url-connection-client"
# )

# for pkg in "${AWS_PACKAGES[@]}"; do
#     export DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
# done

export S3IP=$(cat /etc/hosts | grep host.docker.internal | awk '{print $1}')

export S3_ENDPOINT="http://${S3IP}:9000"

if [ "$S3_URI" != "" ];
then
	export S3_ENDPOINT=$S3_URI
fi

spark-sql --packages $DEPENDENCIES \
    --conf spark.sql.defaultCatalog=${BIOS_CATALOG} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=${POSTGRES_CONNECTION_STRING}\
    --conf spark.sql.catalog.bios.jdbc.user=${POSTGRES_USER} \
    --conf spark.sql.catalog.bios.jdbc.password=${POSTGRES_PASSWORD} \
    --conf spark.sql.catalog.bios.warehouse=${S3_FULL_URL} \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=${S3_ENDPOINT} \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/workspace/spark-events \
    --conf spark.history.fs.logDirectory= /workspace/spark-events \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=4g

#   CREATE TABLE bios.table_01 (
#          id bigint,
#          data string,
#          category string)
#   USING iceberg
#   PARTITIONED BY (category);



#CREATE TABLE bios.default.tabela_particionada02 (id bigint, data string, category string) USING iceberg PARTITIONED BY (category);
#INSERT INTO bios.tabela_particionada02 VALUES (1, 'Testando meus dados', 'teste');
