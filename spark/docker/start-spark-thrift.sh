export AWS_PACKAGES=(
  "bundle"
  "url-connection-client"
)

for pkg in "${AWS_PACKAGES[@]}"; do
    export DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

export HOST_IP=$(cat /etc/hosts | grep host.docker.internal | awk '{print $1}')

## ONLY FOR MY LOCAL TEST ! REMOVE IT !
export AWS_SECRET_ACCESS_KEY=@Senha01
## REMOVE HERE ....

start-thriftserver.sh --packages $DEPENDENCIES \
    --conf iceberg.engine.hive.enabled=true \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.bios.type=hive \
    --conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.bios.uri=jdbc:postgresql://${HOST_IP}:5420/db_iceberg \
    --conf spark.sql.catalog.bios.jdbc.user=icbergcat \
    --conf spark.sql.catalog.bios.jdbc.password=hNXz35UBRcAC \
    --conf spark.sql.catalog.bios.warehouse=s3a://bios/ \
    --conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.bios.s3.endpoint=http://${HOST_IP}:9000 \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.defaultCatalog=spark_catalog \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/workspace/spark-events \
    --conf spark.history.fs.logDirectory= /workspace/spark-events \
    --conf spark.sql.catalogImplementation=in-memory

    #--packages $DEPENDENCIES \
    #--hiveconf hive.metastore.uris=thrift://${IP}:${PORT} \
    #--conf iceberg.engine.hive.enabled=true \
    #--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    #--conf spark.sql.catalog.bios.uri=thrift://${IP}:${PORT} \
    #--conf spark.sql.catalog.bios=org.apache.iceberg.spark.SparkCatalog \
    #--conf spark.sql.catalog.bios.type=hadoop \
    #--conf spark.sql.catalog.bios.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    #--conf spark.sql.catalog.bios.uri=jdbc:postgresql://host.docker.internal:5420/db_iceberg \
    #--conf spark.sql.catalog.bios.jdbc.user=icbergcat \
    #--conf spark.sql.catalog.bios.jdbc.password=hNXz35UBRcAC \
    #--conf spark.sql.catalog.bios.warehouse=s3a://bios/ \
    #--conf spark.sql.catalog.bios.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    #--conf spark.sql.catalog.bios.s3.endpoint=http://${HOST_IP}:9000 \
    #--conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    #--conf spark.sql.catalog.spark_catalog.type=hive \
    #--conf spark.sql.defaultCatalog=bios \
    #--conf spark.eventLog.enabled=true \
    #--conf spark.eventLog.dir=/workspace/spark-events \
    #--conf spark.history.fs.logDirectory= /workspace/spark-events \
    #--conf spark.sql.catalogImplementation=in-memory
    #--conf spark.sql.catalog.bios.type=hive \
    #--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
    #--conf spark.hadoop.fs.s3a.access.key=$key \
    #--conf spark.hadoop.fs.s3a.secret.key=$secret \
