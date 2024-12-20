#!/bin/bash

spark-submit --packages $DEPENDENCIES \
    --conf spark.sql.defaultCatalog=${BIOS_CATALOG} \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=${POSTGRES_CONNECTION_STRING}\
    --conf spark.sql.catalog.bios.jdbc.user=${POSTGRES_USER} \
    --conf spark.sql.catalog.bios.jdbc.password=${POSTGRES_PASSWORD} \
    --conf spark.sql.catalog.bios.warehouse=${S3_FULL_URL} \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=${S3_URI} \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/tmp/spark-events \
    --conf spark.history.fs.logDirectory=/tmp/spark-events \
    --conf spark.sql.catalogImplementation=in-memory \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=4g \
    --conf spark.jars=/jars/* \
    --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" $1 $2 $3 
	
	start-spark-submit.sh: |
      #!/bin/bash
      mkdir -p /tmp/spark-events
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
          --conf spark.sql.catalog.bios.s3.endpoint=${S3_URI} \
          --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
          --conf spark.eventLog.enabled=true \
          --conf spark.eventLog.dir=/tmp/spark-events \
          --conf spark.history.fs.logDirectory=/tmp/spark-events \
          --conf spark.sql.catalogImplementation=in-memory \
          --conf spark.executor.memory=6g \
          --conf spark.driver.memory=4g \
          --conf spark.jars=/jars/* \
          --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" $1 $2 $3


