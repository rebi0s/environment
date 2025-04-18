# inicialização
import os
import sys
import logging
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as FSql
from utils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, DoubleType
from py4j.java_gateway import java_import
from typing import Tuple


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
        .set("spark.sql.catalog.bios.jdbc.schema-version", "V1")
        .set("spark.sql.catalog.bios.warehouse", "s3a://rebios-test-env/")
        .set("spark.sql.catalog.bios.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.bios.s3.endpoint", "https://s3.amazonaws.com/")
        .set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalogImplementation", "in-memory")
        .set("spark.sql.defaultCatalog", os.getenv("CTRNA_CATALOG_DEFAULT","bios")) # Name of the Iceberg catalog
        .set("spark.sql.catalog.bios.database", "rebios") # Nome do banco de dados
        .set("spark.sql.legacy.timeParserPolicy","LEGACY") # [INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER] You may get a different result due to the upgrading to Spark >= 3.0:
    )
    #/opt/spark/conf# vi spark-defaults.conf
    #+ cat spark-defaults.conf
    #spark.executor.memory         16g
    #spark.driver.memory            4g
    #spark.driver.maxResultSize      0
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def configLogger(log_name: str):
    # nome do log é a string que identifica a origem da entrada no log, visto que um mesmo arquivo receberá log de diferentes rotinas/etapas da ingestão
    logger = logging.getLogger(log_name)
    # o logger, como sendo o objeto principal do log, tem o nível de detalhe definido como DEBUG, e no handler de cada logger é definido o nível desejado.
    logger.setLevel(logging.DEBUG)
    return logger

def addLogHandler(idLogger: logging.Logger, log_level: str):
    # Obtém a data atual no formato desejado
    data_atual = datetime.now().strftime('%Y%m%d')
    log_path = os.getenv("CTRNA_LOG_PATH", "/home/src/etl/")
    # Usa a data atual para formatar o nome do arquivo de log
    file_name = f'{log_path}ingestion_{data_atual}.log'
    # todo o processo de ingestão utilizará um mesmo arquivo de log, mas caso seja desejado logs individuais, deve-se fornecer o nome do arquivo diferente.
    handler = logging.FileHandler(file_name)
    formatter = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    if log_level =='DEBUG':
        handler.setLevel(logging.DEBUG)
    if log_level == 'INFO':
        handler.setLevel(logging.INFO)
    if log_level =='WARN':
        handler.setLevel(logging.WARN)
    if log_level =='ERROR':
        handler.setLevel(logging.ERROR)
    if log_level =='CRITICAL':
        handler.setLevel(logging.CRITICAL)
    # o handler é atribuído ao logger para envio das mensagens
    idLogger.addHandler(handler)
    return handler

def execute_sql_commands_from_file(spark, file_path, logger: logging.Logger):
    df = spark.read.text(file_path)
    
    commands = df.rdd.flatMap(lambda x: x[0].split(";")).filter(lambda x: x.strip() != "").collect()
    for command in commands:
        try:
            logger.info(f"Executing command: {command}")
            spark.sql(command)
            logger.info("Command executed.")
        except Exception as e:
            logger.error(f"Error while executing INIT profile on OMOP database: {str(e)}")


def loadPortgreSql(file_path: StringType, table_name: StringType):
    spark = SparkSession.builder.appName("LoadPostgres").config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar").getOrCreate()

    # Lê o arquivo Parquet
    parquet_file_path = file_path
    df = spark.read.parquet("/home/etl-rebios/BR-DWGD_2010.parquet")

    # Configurações do PostgreSQL
    postgres_url = "jdbc:postgresql://host.docker.internal:5432/examples"
    postgres_properties = {"user": "postgres","password": "23mJzsfUfUJqujYc!","driver": "org.postgresql.Driver"}

    # Escreve o DataFrame no PostgreSQL
    df.write.jdbc(url=postgres_url, table="clima_2010", mode="overwrite", properties=postgres_properties)

    # Encerra a sessão Spark
    spark.stop()
