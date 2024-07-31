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


def loadOMOPConcept(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table CONCEPT started.")
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

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

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

        df_concept=spark.createDataFrame(df_load.select(\
        df_load.concept_id, \
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

        df_concept.show()
        df_concept.writeTo("bios.concept").append()
        logger.info("Data succesully written to table CONCEPT")


def loadOMOPConceptClass(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table CONCEPT_CLASS started.")
#CREATE TABLE concept_class (concept_class_id string NOT NULL, concept_class_name string NOT NULL, concept_class_concept_id bigint NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("concept_class_id", StringType(), True), \
    StructField("concept_class_name", StringType(), True), \
    StructField("concept_class_concept_id", LongType(), True) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_concept_class_schema = StructType([ \
        StructField("concept_class_id", StringType(), False), \
        StructField("concept_class_name", StringType(), False), \
        StructField("concept_class_concept_id", LongType(), False) \
        ])

        df_concept_class=spark.createDataFrame(df_load.select(\
        df_load.concept_class_id, \
        df_load.concept_class_name, \
        df_load.concept_class_concept_id \
        ).rdd, df_concept_class_schema)

        df_concept_class.show()
        df_concept_class.writeTo("bios.concept_class").append()
        logger.info("Data succesully written to table CONCEPT_CLASS")

def loadOMOPConceptSynonym(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table CONCEPT_SYNONYM started.")
#CREATE TABLE concept_synonym (concept_id bigint NOT NULL, concept_synonym_name string NOT NULL, language_concept_id bigint NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("concept_id", LongType(), True), \
    StructField("concept_synonym_name", StringType(), True), \
    StructField("language_concept_id", LongType(), True) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_concept_synonym_schema = StructType([ \
        StructField("concept_id", LongType(), False), \
        StructField("concept_synonym_name", StringType(), False), \
        StructField("language_concept_id", LongType(), False) \
        ])

        df_concept_synonym=spark.createDataFrame(df_load.select(\
        df_load.concept_id, \
        df_load.concept_synonym_name, \
        df_load.language_concept_id \
        ).rdd, df_concept_synonym_schema)

        df_concept_synonym.show()
        df_concept_synonym.writeTo("bios.concept_synonym").append()
        logger.info("Data succesully written to table CONCEPT_SYNONYM")

def loadOMOPDrugStrength(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table DRUG_STRENGTH started.")
#CREATE TABLE drug_strength (drug_concept_id bigint NOT NULL, ingredient_concept_id bigint NOT NULL, amount_value float, amount_unit_concept_id bigint, numerator_value float, 
# numerator_unit_concept_id bigint, denominator_value float, denominator_unit_concept_id bigint, box_size  integer, valid_start_date timestamp NOT NULL, 
# valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;

    df_load_schema = StructType([ \
    StructField("drug_concept_id", LongType(), False), \
    StructField("ingredient_concept_id", LongType(), False), \
    StructField("amount_value", DoubleType(), True), \
    StructField("amount_unit_concept_id", LongType(), True), \
    StructField("numerator_value", DoubleType(), True), \
    StructField("numerator_unit_concept_id", LongType(), True), \
    StructField("denominator_value", DoubleType(), True), \
    StructField("denominator_unit_concept_id", LongType(), True), \
    StructField("box_size",  IntegerType(), True), \
    StructField("valid_start_date", IntegerType(), False), \
    StructField("valid_end_date", IntegerType(), False), \
    StructField("invalid_reason", StringType(), True) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("drug_concept_id", LongType(), False), \
        StructField("ingredient_concept_id", LongType(), False), \
        StructField("amount_value", DoubleType(), True), \
        StructField("amount_unit_concept_id", LongType(), True), \
        StructField("numerator_value", DoubleType(), True), \
        StructField("numerator_unit_concept_id", LongType(), True), \
        StructField("denominator_value", DoubleType(), True), \
        StructField("denominator_unit_concept_id", LongType(), True), \
        StructField("box_size",  IntegerType(), True), \
        StructField("valid_start_date", DateType(), False), \
        StructField("valid_end_date", DateType(), False), \
        StructField("invalid_reason", StringType(), True) \
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.drug_concept_id, \
        df_load.ingredient_concept_id, \
        df_load.amount_value, \
        df_load.amount_unit_concept_id, \
        df_load.numerator_value, \
        df_load.numerator_unit_concept_id, \
        df_load.denominator_value, \
        df_load.denominator_unit_concept_id, \
        df_load.box_size, \
        FSql.to_date(FSql.lpad(df_load.valid_start_date,8,'0'), 'yyyyMMdd').alias('valid_start_date'), \
        FSql.to_date(FSql.lpad(df_load.valid_end_date,8,'0'), 'yyyyMMdd').alias('valid_end_date'), \
        df_load.invalid_reason \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.drug_strength").append()
        logger.info("Data succesully written to table DRUG_STRENGTH")

def loadOMOPVocabulary(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table VOCABULARY started.")
#CREATE TABLE vocabulary (vocabulary_id string NOT NULL, vocabulary_name string NOT NULL, vocabulary_reference string, vocabulary_version string, vocabulary_concept_id bigint NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("vocabulary_id", StringType(), False), \
    StructField("vocabulary_name", StringType(), False), \
    StructField("vocabulary_reference", StringType(), True), \
    StructField("vocabulary_version", StringType(), True), \
    StructField("vocabulary_concept_id", LongType(), False) \ 
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("vocabulary_id", StringType(), False), \
        StructField("vocabulary_name", StringType(), False), \
        StructField("vocabulary_reference", StringType(), True), \
        StructField("vocabulary_version", StringType(), True), \
        StructField("vocabulary_concept_id", LongType(), False) \ 
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.vocabulary_id, \
        df_load.vocabulary_name, \
        df_load.vocabulary_reference, \
        df_load.vocabulary_version, \
        df_load.vocabulary_concept_id \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.vocabulary").append()
        logger.info("Data succesully written to table VOCABULARY")


def loadOMOPConceptAncestor(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table CONCEPT_ANCESTOR started.")
#CREATE TABLE concept_ancestor (ancestor_concept_id bigint NOT NULL, descendant_concept_id bigint NOT NULL, min_levels_of_separation integer NOT NULL, max_levels_of_separation integer NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("ancestor_concept_id", LongType(), False),\
    StructField("descendant_concept_id", LongType(), False),\
    StructField("min_levels_of_separation", IntegerType(), False),\
    StructField("max_levels_of_separation", IntegerType(), False) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("ancestor_concept_id", LongType(), False),\
        StructField("descendant_concept_id", LongType(), False),\
        StructField("min_levels_of_separation", IntegerType(), False),\
        StructField("max_levels_of_separation", IntegerType(), False) \
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.ancestor_concept_id, \
        df_load.descendant_concept_id, \
        df_load.min_levels_of_separation, \
        df_load.max_levels_of_separation \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.concept_ancestor").append()
        logger.info("Data succesully written to table CONCEPT_ANCESTOR")


def loadOMOPConceptRelationship(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table CONCEPT_ANCESTOR started.")
#CREATE TABLE concept_relationship (concept_id_1 integer NOT NULL, concept_id_2 integer NOT NULL, relationship_id string NOT NULL, valid_start_date timestamp NOT NULL, valid_end_date timestamp NOT NULL, invalid_reason string ) using iceberg;

    df_load_schema = StructType([ \
    StructField("concept_id_1",  LongType(), False), \
    StructField("concept_id_2", LongType(), False), \
    StructField("relationship_id", StringType(), False), \
    StructField("valid_start_date", IntegerType(), False), \
    StructField("valid_end_date", IntegerType(), False), \
    StructField("invalid_reason", StringType(), True) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("concept_id_1",  LongType(), False), \
        StructField("concept_id_2", LongType(), False), \
        StructField("relationship_id", StringType(), False), \
        StructField("valid_start_date", DateType(), False), \
        StructField("valid_end_date", DateType(), False), \
        StructField("invalid_reason", StringType(), True)
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.concept_id_1, \
        df_load.concept_id_2, \
        df_load.relationship_id, \
        FSql.to_date(FSql.lpad(df_load.valid_start_date,8,'0'), 'yyyyMMdd').alias('valid_start_date'), \
        FSql.to_date(FSql.lpad(df_load.valid_end_date,8,'0'), 'yyyyMMdd').alias('valid_end_date'), \
        df_load.invalid_reason \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.concept_relationship").append()
        logger.info("Data succesully written to table CONCEPT_RELATIONSHIP")

def loadOMOPDomain(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table DOMAIN started.")
#CREATE TABLE domain (domain_id string NOT NULL, domain_name string NOT NULL, domain_concept_id bigint NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("domain_id", StringType(), False), \
    StructField("domain_name", StringType(), False), \
    StructField("domain_concept_id", LongType(), False) \
   ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("domain_id", StringType(), False), \
        StructField("domain_name", StringType(), False), \
        StructField("domain_concept_id", LongType(), False) \
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.domain_id, \
        df_load.domain_name, \
        df_load.domain_concept_id \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.domain").append()
        logger.info("Data succesully written to table DOMAIN")

def loadOMOPRelationship(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
#CONCEPT.csv           CONCEPT_CLASS.csv         CONCEPT_SYNONYM.csv  DRUG_STRENGTH.csv  VOCABULARY.csv  
#CONCEPT_ANCESTOR.csv  CONCEPT_RELATIONSHIP.csv  DOMAIN.csv           RELATIONSHIP.csv
    logger.info("Loading on table RELATIONSHIP started.")
#CREATE TABLE relationship (relationship_id string NOT NULL, relationship_name string NOT NULL, is_hierarchical string NOT NULL, defines_ancestry string NOT NULL, reverse_relationship_id string NOT NULL, relationship_concept_id bigint NOT NULL ) using iceberg;

    df_load_schema = StructType([ \
    StructField("relationship_id", StringType(), False), \
    StructField("relationship_name", StringType(), False), \
    StructField("is_hierarchical", StringType(), False), \
    StructField("defines_ancestry", StringType(), False), \
    StructField("reverse_relationship_id", StringType(), False), \
    StructField("relationship_concept_id", LongType(), False) \
    ])

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep="\t", header=True, schema=df_load_schema)

    if df_load.count() > 0:
        df_iceberg_schema = StructType([ \
        StructField("relationship_id", StringType(), False), \
        StructField("relationship_name", StringType(), False), \
        StructField("is_hierarchical", StringType(), False), \
        StructField("defines_ancestry", StringType(), False), \
        StructField("reverse_relationship_id", StringType(), False), \
        StructField("relationship_concept_id", LongType(), False) \
        ])

        df_iceberg=spark.createDataFrame(df_load.select(\
        df_load.relationship_id, \
        df_load.relationship_name, \
        df_load.is_hierarchical, \
        df_load.defines_ancestry, \
        df_load.reverse_relationship_id, \
        df_load.relationship_concept_id \
        ).rdd, df_iceberg_schema)

        df_iceberg.show()
        df_iceberg.writeTo("bios.relationship").append()
        logger.info("Data succesully written to table RELATIONSHIP")

