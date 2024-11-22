from pyspark.sql import SparkSession

def init_spark():
    spark = SparkSession \
        .builder \
        .appName("DataExtraction") \
        .config("spark.jars.packages",
                "org.postgresql:postgresql:42.6.0,org.apache.iceberg:iceberg-bundled-guava:1.5.0,org.apache.iceberg:iceberg-core:1.5.0,org.apache.iceberg:iceberg-aws:1.5.0,org.apache.iceberg:iceberg-spark:1.5.0,org.apache.iceberg:iceberg-spark-runtime-3.4_2.13:1.5.0,org.apache.iceberg:iceberg-spark-extensions-3.4_2.13:1.5.0,org.apache.iceberg:iceberg-hive-runtime:1.5.0,org.apache.iceberg:iceberg-hive-metastore:1.5.0,org.slf4j:slf4j-simple:2.0.7") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.bios", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.bios.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.bios.uri", "jdbc:postgresql://host.docker.internal:5420/db_iceberg") \
        .config("spark.sql.catalog.bios.jdbc.user", "icbergcat") \
        .config("spark.sql.catalog.bios.jdbc.password", "hNXz35UBRcAC") \
        .config("spark.sql.catalog.bios.warehouse", "s3a://rebios-test-env/rebios") \
        .config("spark.sql.catalog.bios.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.bios.s3.endpoint", "https://s3.amazonaws.com") \
        .config("spark.sql.defaultCatalog", "bios") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "6g") \
        .config("iceberg.engine.hive.enabled", "true") \
        .config("spark.driver.host", "18.220.89.149") \
        .getOrCreate()
    return spark

