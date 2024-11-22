# inicialização
import os
import sys
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as FSql
from utils import init_spark
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, \
    DoubleType, FloatType


def loadStates(spark: SparkSession):
    # load dos estados
    estados = [
        ('Acre', 12, 'AC'),
        ('Alagoas', 27, 'AL'),
        ('Amapá', 16, 'AP'),
        ('Amazonas', 13, 'AM'),
        ('Bahia', 29, 'BA'),
        ('Ceará', 23, 'CE'),
        ('Distrito Federal', 53, 'DF'),
        ('Espírito Santo', 32, 'ES'),
        ('Goiás', 52, 'GO'),
        ('Maranhão', 21, 'MA'),
        ('Mato Grosso', 51, 'MT'),
        ('Mato Grosso do Sul', 50, 'MS'),
        ('Minas Gerais', 31, 'MG'),
        ('Pará', 15, 'PA'),
        ('Paraíba', 25, 'PB'),
        ('Paraná', 41, 'PR'),
        ('Pernambuco', 26, 'PE'),
        ('Piauí', 22, 'PI'),
        ('Rio Grande do Norte', 24, 'RN'),
        ('Rio Grande do Sul', 43, 'RS'),
        ('Rio de Janeiro', 33, 'RJ'),
        ('Rondônia', 11, 'RO'),
        ('Roraima', 14, 'RR'),
        ('Santa Catarina', 42, 'SC'),
        ('São Paulo', 35, 'SP'),
        ('Sergipe', 28, 'SE'),
        ('Tocantins', 17, 'TO'),
    ]
    estados_cols = ["nome_uf", "codigo_uf", "uf"]
    df_estados = spark.createDataFrame(data=estados, schema=estados_cols)
    return df_estados


# Arquivo do IBGE:
# OID_;
# nome;
# geocodigo;
# Longitude;
# Latitude


# uso desse script:
# spark-submit pysparkAirflow.py /home/etl-rebios/etl-files/ lim_municipio_a.csv
# a cada execução serão inseridos 5570 linhas na tabela abaixo.
# criar essa tabela antes de rodar:
# CREATE TABLE bios.rebios.airflowTeste2 (location_id bigint NOT NULL, address_1 string, address_2 string, city string, state string, zip string, county string, location_source_value string, country_concept_id bigint, country_source_value string, latitude float, longitude float ) using iceberg;

file_path = '/opt/airflow/etl-rebios/etl-files' # sys.argv[1]
file_name = 'lim_municipio_a.csv' # sys.argv[2]

spark = init_spark()

df_states = loadStates(spark)

df_load_schema = StructType([ \
    StructField("location_id", LongType(), False), \
    StructField("city", StringType(), True), \
    StructField("state", StringType(), True), \
    StructField("county", StringType(), True), \
    StructField("location_source_value", StringType(), True), \
    StructField("country_concept_id", LongType(), True), \
    StructField("country_source_value", StringType(), True), \
    StructField("latitude", FloatType(), True), \
    StructField("longitude", FloatType(), True), \
    StructField("address_1", StringType(), True), \
    StructField("address_2", StringType(), True), \
    StructField("zip", StringType(), True) \
    ])

#    df_source = pd.read_excel(os.path.join(file_path, file_name))
#    df_input = spark.createDataFrame(df_source)

df_load = spark.read.csv(os.path.join(file_path, file_name), sep=";", header=True, inferSchema=True)

df_load = df_load.withColumn("COD_UF", FSql.substring("geocodigo", 1, 2))

df_load = (df_load.join(df_states, [df_load.COD_UF == df_states.codigo_uf], 'inner'))

df_load = df_load.withColumn("Latitude", FSql.regexp_replace("Latitude", ",", "."))
df_load = df_load.withColumn("Longitude", FSql.regexp_replace("Longitude", ",", "."))

if df_load.count() > 0:
    df_location = spark.createDataFrame(df_load.select( \
        FSql.lit(0).cast(LongType()).alias('location_id'), \
        df_load.nome.alias('city'), \
        df_load.nome_uf.alias('state'), \
        df_load.codigo_uf.cast(StringType()).alias('county'), \
        df_load.geocodigo.alias('location_source_value'), \
        FSql.lit(4075645).cast(LongType()).alias('country_concept_id'), \
        FSql.lit('Brasil').alias('country_source_value'), \
        df_load.Latitude.cast(FloatType()).alias('latitude'), \
        df_load.Longitude.cast(FloatType()).alias('longitude'), \
        FSql.lit(None).cast(StringType()).alias('address_1'), \
        FSql.lit(None).cast(StringType()).alias('address_2'), \
        FSql.lit(None).cast(StringType()).alias('zip') \
        ).rdd, \
                                        df_load_schema)

    if df_location.count() > 0:
        # obtem o max da tabela para usar na inserção de novos registros
        count_max_location_df = spark.sql(
            "SELECT greatest(max(location_id),0) + 1 AS max_location FROM bios.rebios.airflowTeste2")
        count_max_location = count_max_location_df.first().max_location
        # geração dos id's únicos nos dados de entrada. O valor inicial é 1.
        df_location = df_location.withColumn("location_id", monotonically_increasing_id())
        # sincroniza os id's gerados com o max(person_id) existente no banco de dados
        df_location = df_location.withColumn("location_id", df_location["location_id"] + count_max_location)

        # the show command below is to force the dataframe to be checked against its structure field. The error trap is outside this routine.
        df_location.show()
        df_location.writeTo("bios.rebios.airflowTeste2").append()
        df_location.unpersist()


