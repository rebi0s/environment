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

start-thriftserver.sh --packages $DEPENDENCIES \
    --conf spark.sql.defaultCatalog=bios \
    --conf iceberg.engine.hive.enabled=true \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.warehouse=s3a://${S3_BUCKET}/rebios \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=${S3_ENDPOINT} \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=jdbc:postgresql://host.docker.internal:5420/db_iceberg \
    --conf spark.sql.catalog.bios.jdbc.user=icbergcat \
    --conf spark.sql.catalog.bios.jdbc.password=hNXz35UBRcAC \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=4g

#docker exec -it spark_master /bin/bash
#beeline
#!connect jdbc:hive2://localhost:10000/bios
