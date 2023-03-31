mkdir -p /workspace/spark-events | true

export AWS_PACKAGES=(
  "bundle"
  "url-connection-client"
)

for pkg in "${AWS_PACKAGES[@]}"; do
    export DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

spark-sql --packages $DEPENDENCIES \
    --jars $EXTRA_JARS \
#    --conf spark.sql.defaultCatalog=bios \
#    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
#    --conf spark.sql.catalog.bios.warehouse=s3://bios \
#    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
#    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
#    --conf spark.eventLog.enabled=true \
#    --conf spark.eventLog.dir=/workspace/spark-events \
#    --conf spark.history.fs.logDirectory= /workspace/spark-events \
#    --conf spark.sql.catalogImplementation=in-memory

    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=jdbc:postgresql://host.docker.internal:5420/db_iceberg \
    --conf spark.sql.catalog.bios.jdbc.user=icebergcat \
    --conf spark.sql.catalog.bios.jdbc.password=hNXz35UBRcAC \
    --conf spark.sql.catalog.bios.warehouse=s3://bios \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=http://host.docker.internal:9000 \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.defaultCatalog=bios \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/workspace/spark-events \
    --conf spark.history.fs.logDirectory= /workspace/spark-events \
    --conf spark.sql.catalogImplementation=in-memory
