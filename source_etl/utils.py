# inicialização
import os
import sys
import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import *
from utils import *

def initSpark():

    # adding iceberg configs
    conf = (
        SparkConf()
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.bios", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.bios.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .set("spark.sql.catalog.bios.uri", "jdbc:postgresql://host.docker.internal:5420/db_iceberg")
        .set("spark.sql.catalog.bios.jdbc.user", "icbergcat")
        .set("spark.sql.catalog.bios.jdbc.password", "hNXz35UBRcAC")
        .set("spark.sql.catalog.bios.jdbc.schema-version=V1")
        .set("spark.sql.catalog.bios.warehouse", os.getenv("CTRNA_CATALOG_WAREHOUSE", "s3a://bios/"))
        .set("spark.sql.catalog.bios.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.bios.s3.endpoint", os.getenv("CTRNA_CATALOG_S3_ENDPOINT","http://172.17.0.1:9000"))
        .set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.defaultCatalog", os.getenv("CTRNA_CATALOG_DEFAULT","bios")) # Name of the Iceberg catalog
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def utlLogInit(log_file_name: str):
    handler = logging.FileHandler(log_file_name)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

def initLogger(log_name: str, log_level: str, log_handler: logging.FileHandler):
    logger = logging.getLogger(log_name)
    if log_level == 'INFO':
        logger.setLevel(logging.INFO)
    if log_level =='WARN':
        logger.setLevel(logging.WARN)
    if log_level =='DEBUG':
        logger.setLevel(logging.DEBUG)
    if log_level =='ERROR':
        logger.setLevel(logging.ERROR)
    if log_level =='CRITICAL':
        logger.setLevel(logging.CRITICAL)
    logger.addHandler(log_handler)
    return logger


