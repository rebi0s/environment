# inicialização
import os
import sys
import logging
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as FSql
from utils import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType

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

def loadOMOPVocabulary(table_name: str, file_path: str, file_name: str, spark_session: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    if table_name == 'CONCEPT':
        df_load_schema = StructType([ \
        StructField("concept_id", LongType(), False), \
        StructField("concept_name", StringType(), False), \
        StructField("domain_id", StringType(), False), \
        StructField("vocabulary_id", StringType(), False), \
        StructField("concept_class_id", StringType(), False), \
        StructField("standard_concept", StringType(), True), \
        StructField("concept_code", StringType(), False), \
        StructField("valid_start_date", IntegerType(), False), \
        StructField("valid_end_date", IntegerType(), False), \
        StructField("invalid_reason", StringType(), True) \
        ])

        df_load = spark_session.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

        # *************************************************************
        # Input format: CONCEPT.CSV
        # root
        #  |-- concept_id: string (nullable = true)
        #  |-- concept_name: string (nullable = true)
        #  |-- domain_id: string (nullable = true)
        #  |-- vocabulary_id: string (nullable = true)
        #  |-- concept_class_id: string (nullable = true)
        #  |-- standard_concept: string (nullable = true)
        #  |-- concept_code: string (nullable = true)
        #  |-- valid_start_date: string (nullable = true)
        #  |-- valid_end_date: string (nullable = true)
        #  |-- invalid_reason: string (nullable = true)
        # *************************************************************

        if df_load.count() > 0:
            df_concept_schema = StructType([ \
            StructField("concept_id", LongType(), False), \
            StructField("concept_name", StringType(), False), \
            StructField("domain_id", StringType(), False), \
            StructField("vocabulary_id", StringType(), False), \
            StructField("concept_class_id", StringType(), False), \
            StructField("standard_concept", StringType(), True), \
            StructField("concept_code", StringType(), False), \
            StructField("valid_start_date", DateType(), False), \
            StructField("valid_end_date", DateType(), False), \
            StructField("invalid_reason", StringType(), True) \
            ])

            df_concept=spark_session.createDataFrame(df_load.select(df_load.concept_id, \
            df_load.concept_name, \
            df_load.domain_id, \
            df_load.vocabulary_id, \
            df_load.concept_class_id, \
            df_load.standard_concept, \
            df_load.concept_code, \
            FSql.to_date(FSql.lpad(df_load.valid_start_date,8,'0'), 'yyyyMMdd').alias('valid_start_date'), \
            FSql.to_date(FSql.lpad(df_load.valid_end_date,8,'0'), 'yyyyMMdd').alias('valid_end_date'), \
            df_load.invalid_reason \
            ).rdd, df_concept_schema)

            try:
                df_concept.show()
                df_concept.writeTo("bios.concept").append()
            except Exception as e:
                logger.error("Error on writing data to OMOP Vocabulary:", str(e))



