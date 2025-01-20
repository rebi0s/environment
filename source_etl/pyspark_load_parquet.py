# inicialização
import os
import sys
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as FSql
from utils import *
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, DoubleType, FloatType

def loadStates(spark: SparkSession):
    #load dos estados
    estados = [
    ('Acre',12,'AC'),
    ('Alagoas',27,'AL'),
    ('Amapá',16,'AP'),
    ('Amazonas',13,'AM'),
    ('Bahia',29,'BA'),
    ('Ceará',23,'CE'),
    ('Distrito Federal',53,'DF'),
    ('Espírito Santo',32,'ES'),
    ('Goiás',52,'GO'),
    ('Maranhão',21,'MA'),
    ('Mato Grosso',51,'MT'),
    ('Mato Grosso do Sul',50,'MS'),
    ('Minas Gerais',31,'MG'),
    ('Pará',15,'PA'),
    ('Paraíba',25,'PB'),
    ('Paraná',41,'PR'),
    ('Pernambuco',26,'PE'),
    ('Piauí',22,'PI'),
    ('Rio Grande do Norte',24,'RN'),
    ('Rio Grande do Sul',43,'RS'),
    ('Rio de Janeiro',33,'RJ'),
    ('Rondônia',11,'RO'),
    ('Roraima',14,'RR'),
    ('Santa Catarina',42,'SC'),
    ('São Paulo',35,'SP'),
    ('Sergipe',28,'SE'),
    ('Tocantins',17,'TO'),
    ]
    estados_cols = ["nome_uf","codigo_uf","uf"]
    df_estados = spark.createDataFrame(data=estados, schema = estados_cols)
    return df_estados

#uso desse script:
#spark-submit pyspark_load_parquet.py /home/etl-rebios/etl-files/ BR-DWGD_all_2010_2022_32bits.parquet
#criar essa tabela antes de rodar:
#create table bios.rebios.clima_flat (
#code_muni VARCHAR,   
#abbrev_state VARCHAR,   
#name_state VARCHAR,   
#code_region BIGINT,   
#name_region VARCHAR,   
#date TIMESTAMP,   
#TMIN_mean FLOAT,   
#TMAX_mean FLOAT,   
#PR_mean FLOAT,   
#ETo_mean FLOAT,   
#RH_mean FLOAT,   
#Rs_mean FLOAT,   
#u2_mean FLOAT,   
#TMIN_min FLOAT,   
#TMAX_min FLOAT,   
#PR_min FLOAT,   
#ETo_min FLOAT,   
#RH_min FLOAT,   
#Rs_min FLOAT,   
#u2_min FLOAT,   
#TMIN_max FLOAT,   
#TMAX_max FLOAT,   
#PR_max FLOAT,  
#ETo_max FLOAT,   
#RH_max FLOAT,   
#Rs_max FLOAT,   
#u2_max FLOAT,   
#TMIN_stdev FLOAT,   
#TMAX_stdev FLOAT,   
#PR_stdev FLOAT,   
#ETo_stdev FLOAT,   
#RH_stdev FLOAT,   
#Rs_stdev FLOAT,   
#u2_stdev FLOAT,   
#PR_sum FLOAT,   
#ETo_sum FLOAT,   
#n_days_TN10p BIGINT,   
#n_days_TX90p BIGINT,   
#n_days_TX_gd_35 BIGINT,   
#index_level_0__ BIGINT   ) using iceberg;

#code_muni VARCHAR   
#abbrev_state VARCHAR   
#name_state VARCHAR   
#code_region BIGINT   
#name_region VARCHAR   
#date TIMESTAMP   
#TMIN_mean FLOAT   
#TMAX_mean FLOAT   
#PR_mean FLOAT   
#ETo_mean FLOAT   
#RH_mean FLOAT   
#Rs_mean FLOAT   
#u2_mean FLOAT   
#TMIN_min FLOAT   
#TMAX_min FLOAT   
#PR_min FLOAT   
#ETo_min FLOAT   
#RH_min FLOAT   
#Rs_min FLOAT   
#u2_min FLOAT   
#TMIN_max FLOAT   
#TMAX_max FLOAT   
#PR_max FLOAT   
#ETo_max FLOAT   
#RH_max FLOAT   
#Rs_max FLOAT   
#u2_max FLOAT   
#TMIN_stdev FLOAT   
#TMAX_stdev FLOAT   
#PR_stdev FLOAT   
#ETo_stdev FLOAT   
#RH_stdev FLOAT   
#Rs_stdev FLOAT   
#u2_stdev FLOAT   
#PR_sum FLOAT   
#ETo_sum FLOAT   
#n_days_TN10p BIGINT   
#n_days_TX90p BIGINT   
#n_days_TX_gd_35 BIGINT   
#__index_level_0__ BIGINT   

file_path = sys.argv[1]
file_name = sys.argv[2]

spark = initSpark()

df_states = loadStates(spark)

df_clima_flat_schema = StructType([ \
    StructField("code_muni", StringType(), True), \
    StructField("abbrev_state", StringType(), True), \
    StructField("name_state", StringType(), True), \
    StructField("code_region", StringType(), True), \
    StructField("name_region", StringType(), True), \
    StructField("date", TimestampType(), True), \
    StructField("TMIN_mean", StringType(), True), \
    StructField("TMAX_mean", StringType(), True), \
    StructField("PR_mean", StringType(), True), \
    StructField("ETo_mean", StringType(), True), \
    StructField("RH_mean", StringType(), True), \
    StructField("Rs_mean", StringType(), True), \
    StructField("u2_mean", StringType(), True), \
    StructField("TMIN_min", StringType(), True), \
    StructField("TMAX_min", StringType(), True), \
    StructField("PR_min", StringType(), True), \
    StructField("ETo_min", StringType(), True), \
    StructField("RH_min", StringType(), True), \
    StructField("Rs_min", StringType(), True), \
    StructField("u2_min", StringType(), True), \
    StructField("TMIN_max", StringType(), True), \
    StructField("TMAX_max", StringType(), True), \
    StructField("PR_max", StringType(), True), \
    StructField("ETo_max", StringType(), True), \
    StructField("RH_max", StringType(), True), \
    StructField("Rs_max", StringType(), True), \
    StructField("u2_max", StringType(), True), \
    StructField("TMIN_stdev", StringType(), True), \
    StructField("TMAX_stdev", StringType(), True), \
    StructField("PR_stdev", StringType(), True), \
    StructField("ETo_stdev", StringType(), True), \
    StructField("RH_stdev", StringType(), True), \
    StructField("Rs_stdev", StringType(), True), \
    StructField("u2_stdev", StringType(), True), \
    StructField("PR_sum", StringType(), True), \
    StructField("ETo_sum", StringType(), True), \
    StructField("n_days_TN10p", StringType(), True), \
    StructField("n_days_TX90p", StringType(), True), \
    StructField("n_days_TX_gd_35", StringType(), True), \
    StructField("index_level_0__", StringType(), True) \
])

#    df_source = pd.read_excel(os.path.join(file_path, file_name))
#    df_input = spark.createDataFrame(df_source)

df_load = spark.read.parquet(os.path.join(file_path, file_name), sep=";", header=True, inferSchema=True)

if df_load.count() > 0:
    df_location = spark.createDataFrame(df_load.select(\
    df_load.code_muni, \
    df_load.abbrev_state, \
    df_load.name_state, \
    df_load.code_region, \
    df_load.name_region, \
    df_load.date, \
    df_load.TMIN_mean, \
    df_load.TMAX_mean, \
    df_load.PR_mean, \
    df_load.ETo_mean, \
    df_load.RH_mean, \
    df_load.Rs_mean, \
    df_load.u2_mean, \
    df_load.TMIN_min, \
    df_load.TMAX_min, \
    df_load.PR_min, \
    df_load.ETo_min, \
    df_load.RH_min, \
    df_load.Rs_min, \
    df_load.u2_min, \
    df_load.TMIN_max, \
    df_load.TMAX_max, \
    df_load.PR_max, \
    df_load.ETo_max, \
    df_load.RH_max, \
    df_load.Rs_max, \
    df_load.u2_max, \
    df_load.TMIN_stdev, \
    df_load.TMAX_stdev, \
    df_load.PR_stdev, \
    df_load.ETo_stdev, \
    df_load.RH_stdev, \
    df_load.Rs_stdev, \
    df_load.u2_stdev, \
    df_load.PR_sum, \
    df_load.ETo_sum, \
    df_load.n_days_TN10p, \
    df_load.n_days_TX90p, \
    df_load.n_days_TX_gd_35, \
    df_load.index_level_0__, \
    ).rdd, \
    df_clima_flat_schema)
    
    if df_location.count() > 0:
        # the show command below is to force the dataframe to be checked against its structure field. The error trap is outside this routine.
        df_location.show()   
        df_location.writeTo("bios.rebios.clima_flat").append()
        df_location.unpersist()

