# inicialização
import os
import logging
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
import pyspark.sql.functions as sqlLib


# adding iceberg configs
conf = (
    SparkConf()
    .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .set("spark.sql.catalog.bios", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.bios.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .set("spark.sql.catalog.bios.warehouse", "s3a://bios/")
    .set("spark.sql.catalog.bios.s3.endpoint", "http://host.docker.internal:9000")
    .set("spark.sql.defaultCatalog", "bios") # Name of the Iceberg catalog
    .set("spark.sql.catalogImplementation", "in-memory")
    .set("spark.sql.catalog.bios.type", "hadoop") # Iceberg catalog type
    .set("spark.executor.heartbeatInterval", "300000")
    .set("spark.network.timeout", "400000")
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Disable below line to see INFO logs
spark.sparkContext.setLogLevel("ERROR")

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "openlakeuser"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "openlakeuser"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.getenv("ENDPOINT", "host.docker.internal:50000"))
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")

spark = SparkSession.builder.master("local[*]") \
                    .appName('Rebios') \
                    .getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("rebiosparkJob")

logger.info("Inicialização.")

#ordem de inserção segundo a dependência entre as tabelas
#OMOP Version 5.4
#CONCEPT
#CONCEPT_CLASS
#VOCABULARY
#DOMAIN
#LOCATION
#CARE_SITE
#PROVIDER
#PERSON
#VISIT_OCCURRENCE
#VISIT_DETAIL
#RELATIONSHIP
#EPISODE
#NOTE
#CDM_SOURCE
#COHORT_DEFINITION
#CONCEPT_ANCESTOR
#CONCEPT_SYNONYM
#CONDITION_ERA
#DRUG_ERA
#EPISODE_EVENT
#OBSERVATION_PERIOD
#CONCEPT_RELATIONSHIP
#DOSE_ERA
#FACT_RELATIONSHIP
#METADATA
#DEATH
#NOTE_NLP
#SOURCE_TO_CONCEPT_MAP
#COST
#DRUG_STRENGTH
#SPECIMEN
#CONDITION_OCCURRENCE
#DRUG_EXPOSURE
#PROCEDURE_OCCURRENCE
#DEVICE_EXPOSURE
#PAYER_PLAN_PERIOD
#OBSERVATION
#MEASUREMENT
#COHORT


#load dos municípios fazendo download de uma base pública
municipios = [
(1200013,120001,12,'AC','Acrelândia'),
(1200054,120005,12,'AC','Assis Brasil'),
(1200104,120010,12,'AC','Brasiléia'),
(1200138,120013,12,'AC','Bujari'),
(1200179,120017,12,'AC','Capixaba')
]
municipios_cols = ["codigo","cod_semdv","cod_uf","uf","nome"]
df_municipios = spark.createDataFrame(data=municipios, schema = municipios_cols)

cnes_tpunid = [
(69,'CENTRO DE ATENCAO HEMOTERAPIA E OU HEMATOLOGICA',69000),
(70,'CENTRO DE ATENCAO PSICOSSOCIAL',70000),
(71,'CENTRO DE APOIO A SAUDE DA FAMILIA',71000),
(72,'UNIDADE DE ATENCAO A SAUDE INDIGENA',72000),
(1,'POSTO DE SAUDE',1000),
(2,'CENTRO DE SAUDE/UNIDADE BASICA',2000),
(4,'POLICLINICA',4000),
(22,'CONSULTORIO ISOLADO',22000),
(40,'UNIDADE MOVEL TERRESTRE',40000),
(42,'UNIDADE MOVEL DE NIVEL PRE-HOSPITALAR NA AREA DE URGENCIA',42000),
(32,'UNIDADE MOVEL FLUVIAL',32000),
(36,'CLINICA/CENTRO DE ESPECIALIDADE',36000),
(64,'CENTRAL DE REGULACAO DE SERVICOS DE SAUDE',64000),
(43,'FARMACIA',43000),
(39,'UNIDADE DE APOIO DIAGNOSE E TERAPIA (SADT ISOLADO)',39000),
(61,'CENTRO DE PARTO NORMAL - ISOLADO',61000),
(62,'HOSPITAL/DIA - ISOLADO',62000),
(15,'UNIDADE MISTA',15000),
(20,'PRONTO SOCORRO GERAL',20000),
(21,'PRONTO SOCORRO ESPECIALIZADO',21000),
(5,'HOSPITAL GERAL',5000),
(7,'HOSPITAL ESPECIALIZADO',7000),
(60,'COOPERATIVA OU EMPRESA DE CESSAO DE TRABALHADORES NA SAUDE',60000),
(50,'UNIDADE DE VIGILANCIA EM SAUDE',50000),
(67,'LABORATORIO CENTRAL DE SAUDE PUBLICA LACEN',67000),
(68,'CENTRAL DE GESTAO EM SAUDE',68000),
(73,'PRONTO ATENDIMENTO',73000),
(74,'POLO ACADEMIA DA SAUDE',74000),
(84,'CENTRAL DE ABASTECIMENTO',84000),
(85,'CENTRO DE IMUNIZACAO',85000),
(76,'CENTRAL DE REGULACAO MEDICA DAS URGENCIAS',76000),
(79,'OFICINA ORTOPEDICA',79000),
(81,'CENTRAL DE REGULACAO DO ACESSO',81000),
(83,'POLO DE PREVENCAO DE DOENCAS E AGRAVOS E PROMOCAO DA SAUDE',83000),
(82,'CENTRAL DE NOTIFICACAO,CAPTACAO E DISTRIB DE ORGAOS ESTADUAL',82000),
(77,'SERVICO DE ATENCAO DOMICILIAR ISOLADO(HOME CARE)',77000),
(75,'TELESSAUDE',75000),
(80,'LABORATORIO DE SAUDE PUBLICA',80000),
(78,'UNIDADE DE ATENCAO EM REGIME RESIDENCIAL',78000),
]
cnes_tpunid_cols = ["codigo","nome","conceptid"]
df_cnes_tpunid = spark.createDataFrame(data=cnes_tpunid, schema = cnes_tpunid_cols)

#load do cid10 com vocabulário do omop
cid10 = [
('R19.3',45606798)    
]
cid10_cols = ["codigo_cid10", "conceptid"]
df_cid10 = spark.createDataFrame(data=cid10, schema = cid10_cols)

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
estados_cols = ["nome","codigo","uf"]
df_estados = spark.createDataFrame(data=estados, schema = estados_cols)

#load dos estabelecimentos de saúde CNES. Cada establecimento de saúde é uma location que se repete no care_site visto que não temos dados das divisões/unidades dos estabelecimentos de saúde.
#"1200452000725";"2000725";"04034526000143";"3";"3";"SECRETARIA DE ESTADO DE SAUDE";"HOSPITAL DR ARY RODRIGUES";"AV SENADOR EDUARDO ASSMAR";"153";"";"COHAB";"69925000";"001";"";"";"";"(68)3232 2956";"";"hospitalaryrodrigues201705@gmail.com";"";"04034526001115";"04";"03";"";"";"";"";"";"05";"06";"12";"120045";"27/03/2024";"SCNES";"63786311234";"";"";"";"";"-10.151";"-67.736";"11/07/2019";"SCNES";"1023";"S";"";"S";"";"";"E";"30/10/2001";"006";"009";"";""

#CREATE TABLE location (
#			location_id integer ,
#			address_1 varchar(50) NULL,
#			address_2 varchar(50) NULL,
#			city varchar(50) NULL,
#			state varchar(2) NULL,
#			zip varchar(9) NULL,
#			county varchar(20) NULL,
#			location_source_value varchar(50) NULL,
#			country_concept_id integer NULL,
#			country_source_value varchar(80) NULL,
#			latitude float NULL,
#			longitude float NULL );

#carga dos dados do parquet do SINASC
df_sinasc = spark.read.parquet("/home/warehouse/sinasc_2010_2022.parquet")

################################
# inserção do location de cada município de entrada como sendo o endereço da person. 
# Linhas de location adicionais serão criadas para conter os demais municípios de entrada, como CODMUNCART, CODMUNNASC, CODMUNNATU, considerando todas as colunas de município.
# Foi adotado dentro do projeto que o município de nascimento será o de endereço da mãe (CODMUNRES).
################################

#por ser PK será utilizado o código completo do munícipio com os dígitos do estado do início do código. o último dígito é o código verificador. apenas o código do munícipio gera repetição.
#código do Brazil obtido no Athena do vocabulario SNOMED. Vai ser necessário um tratamento para os casos envolvendo estrangeiros. No sinasc o campo CODPAISRES vem com 1 para os brasileiros.
spark.sql("""insert into location ( 
            location_id integer , 
			address_1  ,           
			address_2  ,          
			city  ,                
			state ,               
			zip ,                 
			county  ,             
			location_source_value  ,  
			country_concept_id  ,     
			country_source_value ,    
			latitude,                 
			longitude )
values(
df_location.identity,  
null,
null,
df_municipios.where(sqlLib.col('codigo').rlike('|'.join(df_sinasc.codmunres))),
df_estados.where(sqlLib.col('codigo').rlike('|'.join(substr(df_sinasc.codmunres, 1, 2)))),
null,
null,
df_sinasc.codmunres,
4075645,    
df_sinasc.codpaisres,
null,
null
)""")

cnes = [
(1200452000725,2000725,'HOSPITAL DR ARY RODRIGUES', 'HOSPITAL GERAL', 05, 120045, 'AV SENADOR EDUARDO ASSMAR, 153 COHAB', 69925000, -10.151, -67.736, null)  #retorna o conceptid do tipo da unidade do CNES
]
cnes_cols = ["codigo_unidade","codigo_cnes","nome","nome_tipo","tpunid","codigo_munic","endereco","cep","latitude","longitude","tipo_unid_concept_id"]
df_cnes = spark.createDataFrame(data=cnes, schema = cnes_cols)

################################
# antes da inserção do estabelecimento, atualiza o df_cnes com o concept_id do código do tipo da unidade a partir do df_cnes_tpunid
################################
df_cnes = df_cnes_tpunid.where(sqlLib.col('codigo').rlike('|'.join(df_cnes.tpunid,'.'))) # retorna o conceptid do tipo da unidade 


################################
# inserção do location de cada estabelecimento de saúde
# esse registro é duplicado na tabela care_site por falta de informação no SIM/SINASC
################################
#por ser PK será utilizado o código completo do munícipio com os dígitos do estado do início do código. o último dígito é o código verificador. apenas o código do munícipio gera repetição.
#código do Brazil obtido no Athena do vocabulario SNOMED. Vai ser necessário um tratamento para os casos envolvendo estrangeiros. No sinasc o campo CODPAISRES vem com 1 para os brasileiros.

# retornar o df_cnes.endereco
#retornar o df_cnes.codigo_munic
#retornar o substr(df_cnes.codigo_munic, 1, 2)
#retornar o df_cnes.cep
#retornar o df_cnes.latitude
#retornar o df_cnes.longitude

spark.sql("""insert into location (
            location_id integer ,
			address_1  ,
			address_2  ,
			city  ,
			state ,
			zip ,
			county  ,
			location_source_value  ,
			country_concept_id  ,
			country_source_value ,
			latitude,
			longitude)
values(
df_location.identity,  
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
null,
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
null,
replace(df_sinasc.codestab,'.'),
4075645,    
df_sinasc.codpaisres,
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))) 
)""")

#CREATE TABLE care_site (
#			care_site_id integer ,
#			care_site_name varchar(255) NULL,
#			place_of_service_concept_id integer NULL,
#			location_id integer NULL,
#			care_site_source_value varchar(50) NULL,
#			place_of_service_source_value varchar(50) NULL );

# os estabelecimentos de saúde serão cadastrados em location e repetidos como care_site por falta de detalhes no SIM/SINASC. O care_site terá FK do location.
spark.sql("""insert into care_site(
			care_site_id,
			care_site_name,
			place_of_service_concept_id,
			location_id,
			care_site_source_value,
			place_of_service_source_value)
values
(3, 'Nascimento no Domicílio', 43021744, null, null, 'Domicílio')""")   #43021744 Born at home
spark.sql("""insert into care_site(
			care_site_id,
			care_site_name,
			place_of_service_concept_id,
			location_id,
			care_site_source_value,
			place_of_service_source_value)
values
(4, "Nascimento em Outros Locais", 45881550, null, null, "Outros")""") #45881550 Place of birth unknown
#retorna o nome do estabelecimento
#retorna o concept_id do tipo da unidade do estabelcimento. essa correspondência foi feita no df_cnes_tpunid.
# obtém o location_id com o endereço gerado para o respectivo estabelecimento de saúde na tabela location
spark.sql("""insert into care_site(
			care_site_id,
			care_site_name,
			place_of_service_concept_id,
			location_id,
			care_site_source_value,
			place_of_service_source_value)
values
(
df_care_site.identity,
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
df_cnes.where(sqlLib.col('codigo_cnes').rlike('|'.join(replace(df_sinasc.codestab,'.')))), 
(select location_id from location where location_source_value = replace(df_sinasc.codestab,'.')), 
replace(df_sinasc.codestab,'.'),
null
)""")


# load do provider. Serão criados os valores genéricos de entrada. Valores: 1– Médico; 2– Enfermeira/obstetriz; 3– Parteira; 4– Outros; 9– Ignorado
#CREATE TABLE provider (
#			provider_id integer NOT NULL,
#			provider_name varchar(255) NULL,
#			npi varchar(20) NULL,
#			dea varchar(20) NULL,
#			specialty_concept_id integer NULL,
#			care_site_id integer NULL,
#			year_of_birth integer NULL,
#			gender_concept_id integer NULL,
#			provider_source_value varchar(50) NULL,
#			specialty_source_value varchar(50) NULL,
#			specialty_source_concept_id integer NULL,
#			gender_source_value varchar(50) NULL,
#			gender_source_concept_id integer NULL );

#Valor 1 mapeado para 4000621 [Obstetrician]
#Valor 2 mapeado para 32581 [Nurse]
#Valor 3 mapeado para 40561317 [Midwife]
#Valor 4 mapear com vocabulário do Climaterna
#Valor 9 mapear com vocabulário do Climaterna

#registro do provider
#   rever esse insert para ser provider (df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpnascassi = 1 then 4000621 when df_sinasc.tpnascassi = 2 then 32581 when df_sinasc.tpnascassi = 3 then 40561317 when df_sinasc.tpnascassi = 4 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpnascassi), # TPNASCASSI	Nascimento foi assistido por? Valores: 1– Médico; 2– Enfermeira/obstetriz; 3– Parteira; 4– Outros; 9– Ignorado

spark.sql("""insert into provider (
			provider_id,
			provider_name,
			specialty_source_value,
)
values 
(df_provider.identity, 'Médico', 1)""")
spark.sql("""insert into provider (
			provider_id,
			provider_name,
			specialty_source_value,
)
values (df_provider.identity, 'Enfermeira/obstetriz', 2)""")
spark.sql("""insert into provider (
			provider_id,
			provider_name,
			specialty_source_value,
)
values (df_provider.identity, 'Parteira', 3)""")
spark.sql("""insert into provider (
			provider_id,
			provider_name,
			specialty_source_value,
)
values (df_provider.identity, 'Outros', 4)""")
spark.sql("""insert into provider (
			provider_id,
			provider_name,
			specialty_source_value,
)
values (df_provider.identity, 'Ignorado', 9)""")


#CREATE TABLE person (
#			person_id integer ,
#			gender_concept_id integer ,
#			year_of_birth integer ,
#			month_of_birth integer NULL,
#			day_of_birth integer NULL,
#			birth_datetime datetime NULL,
#			race_concept_id integer ,
#			ethnicity_concept_id integer ,
#			location_id integer NULL,
#			provider_id integer NULL,
#			care_site_id integer NULL,
#			person_source_value varchar(50) NULL,
#			gender_source_value varchar(50) NULL,
#			gender_source_concept_id integer NULL,
#			race_source_value varchar(50) NULL,
#			race_source_concept_id integer NULL,
#			ethnicity_source_value varchar(50) NULL,
#			ethnicity_source_concept_id integer NULL );


# registro do recém nascido
#esse location é o do endereço da person. foi mapeado para o location do código de município de nascimento.
#se refere ao profissional que deu entrada no atendimento. no nosso do sinasc e sim não tem.
#indica onde foi feito o primeiro atendimento, no caso do sinasc e sim é o estabelecimento de saúde da ocorrência

#usando row_number() no dataframe de input, mas precisa ajustar com o masx(id) existente no banco. como evitar a duplicidade?
# sexo não informado foi mapeado para unknown 
# a raça foi mapeado para white, black, Asian-ethnic group, black-other mixed, brazilian indians 
# o omop mapeia apenas duas variações: "hispânico" e "não hispânico". Esse é o código do "hispânico".
# cada município terá um location_id com source_value contendo o código original e completo do município.
# 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
#não existe um id para person fornecido pelo SINASC
#o sinasc não fornece um valor para etinia

spark.sql("""insert into person 
(
			person_id  ,
			gender_concept_id  ,
			year_of_birth  ,
			month_of_birth  ,
			day_of_birth  ,
			birth_datetime  ,
			race_concept_id  ,
			ethnicity_concept_id  ,
			location_id  ,   
			provider_id  ,   
			care_site_id  ,  
			person_source_value ,
			gender_source_value ,
			gender_source_concept_id  ,
			race_source_value ,
			race_source_concept_id  ,
			ethnicity_source_value ,
			ethnicity_source_concept_id   )
values
(
df_sinasc.identity ,  
case when df_sinasc.sexo = 'M' then 8507 when df_sinasc.sexo = 'F' then 8532 else 8551 end,  
year(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
month(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
day(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
make_timestamp(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2), substr(df_sinasc.horanasc,1,2),  substr(df_sinasc.horanasc,3)) ,
case when df_sinasc.racacor = 1 then 3212942 when df_sinasc.racacor = 2 then 3213733 when df_sinasc.racacor = 3 then 3213498 when df_sinasc.racacor = 4 then 3213487 else 3213694 end,  
38003563, 
(select location_id from location where location_source_value = df_sinasc.codmunres), 
NULL, #provider é nulo porque ele será vinculado a ocorrência do parto e não a pessoa
(select care_site_id from care_site where care_site_source_value = replace(df_sinasc.codestab,'.')),
NULL, 
df_sinasc.sexo,
NULL,
df_sinasc.racacor,  
NULL,
NULL, 
NULL)""")


#registro do observation_period do parto
#CREATE TABLE observation_period (
#			observation_period_id integer ,
#			person_id integer ,
#			observation_period_start_date date ,
#			observation_period_end_date date ,
#			period_type_concept_id integer  );

spark.sql("""insert into observation_period (
			observation_period_id,
			person_id integer,
			observation_period_start_date,
			observation_period_end_date,
			period_type_concept_id)
values
(df_sinasc_obs.identity,
 df_sinasc.identity,
 makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)),
 makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)),
 4193440  #notification of Birth obtido no vocabulario do Athena
)""")

#registro do condition_occurrence
#CREATE TABLE condition_occurrence (
#			condition_occurrence_id integer NOT NULL,
#			person_id integer NOT NULL,
#			condition_concept_id integer NOT NULL,
#			condition_start_date date NOT NULL,
#			condition_type_concept_id integer NOT NULL,
#			condition_start_datetime datetime NULL,
#			condition_end_date date NULL,
#			condition_end_datetime datetime NULL,
#			condition_status_concept_id integer NULL,
#			stop_reason varchar(20) NULL,
#			provider_id integer NULL,
#			visit_occurrence_id integer NULL,
#			visit_detail_id integer NULL,
#			condition_source_value varchar(50) NULL,
#			condition_source_concept_id integer NULL,
#			condition_status_source_value varchar(50) NULL );


spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.stdnepidem = 1 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.stdnepidem)""") # STDNEPIDEM	Status de DN Epidemiológica. Valores: 1 – SIM; 0 – NÃO.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpapresent = 1 then 9999999 when df_sinasc.tpapresent = 2 then 9999999 when df_sinasc.tpapresent = 3 then 4218938 else 9999999 , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpapresent)""") # TPAPRESENT	Tipo de apresentação do RN. Valores: 1– Cefálico; 2– Pélvica ou podálica; 3– Transversa; 9– Ignorado.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, 1576063, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.semagestac)""") # SEMAGESTAC	Número de semanas de gestação.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, df_cid10.where(sqlLib.col('codigo_cid10').rlike('|'.join(replace(df_sinasc.codanomal,'.')))), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codanomal)""") # CODANOMAL	Código da anomalia (CID 10). a consulta ao dataframe do cid10 deve retornar o concept_id correspondente ao cid10 de entrada.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, 4072438, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.dtultmenst)""") # DTULTMENST	Data da última menstruação (DUM): dd mm aaaa
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.gestacao)""")  # GESTACAO	Semanas de gestação: 1– Menos de 22 semanas; 2– 22 a 27 semanas; 3– 28 a 31 semanas; 4– 32 a 36 semanas; 5– 37 a 41 semanas; 6– 42 semanas e mais; 9– Ignorado.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.gravidez = 1 then 4014295 when df_sinasc.gravidez = 2 then 4101844 when df_sinasc.gravidez = 3 then 4094046 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.gravidez)""") # GRAVIDEZ	Tipo de gravidez: 1– Única; 2– Dupla; 3– Tripla ou mais; 9– Ignorado.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, 4313474, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.consprenat)""")  # CONSPRENAT	Número de consultas pré‐natal
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, 4313474, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.consultas)""") # CONSULTAS	Número de consultas de pré‐natal. Valores: 1– Nenhuma; 2– de 1 a 3; 3– de 4 a 6; 4– 7 e mais; 9– Ignorado.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.mesprenat)""") # MESPRENAT	Mês de gestação em que iniciou o pré‐natal
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.stcesparto)""") # STCESPARTO	Cesárea ocorreu antes do trabalho de parto iniciar? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.kotelchuck)""") # KOTELCHUCK	
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.paridade)""") # PARIDADE	
spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
values
(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpmetestim)""") # TPMETESTIM	Método utilizado. Valores: 1– Exame físico; 2– Outro método; 9– Ignorado.


# registro da procedure_occurrence
#CREATE TABLE procedure_occurrence (
#			procedure_occurrence_id integer NOT NULL,
#			person_id integer NOT NULL,
#			procedure_concept_id integer NOT NULL,
#			procedure_date date NOT NULL,
#			procedure_datetime datetime NULL,
#			procedure_end_date date NULL,
#			procedure_end_datetime datetime NULL,
#			procedure_type_concept_id integer NOT NULL,
#			modifier_concept_id integer NULL,
#			quantity integer NULL,
#			provider_id integer NULL,
#			visit_occurrence_id integer NULL,
#			visit_detail_id integer NULL,
#			procedure_source_value varchar(50) NULL,
#			procedure_source_concept_id integer NULL,
#			modifier_source_value varchar(50) NULL );

spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
values (
(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.parto = 1 then 999999 when df_sinasc.parto = 2 then 4015701 else 9999999), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.parto)""") # PARTO	Tipo de parto: 1– Vaginal; 2– Cesário; 9– Ignorado
spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
values (
(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.sttrabpart = 1 then 4121586 when df_sinasc.sttrabpart = 2 then 9999999 when df_sinasc.sttrabpart = 3 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.sttrabpart)""") # STTRABPART	Trabalho de parto induzido? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.


# resgistro do measurement
#CREATE TABLE measurement (
#			measurement_id integer NOT NULL,
#			person_id integer NOT NULL,
#			measurement_concept_id integer NOT NULL,
#			measurement_date date NOT NULL,
#			measurement_type_concept_id integer NOT NULL,
#			value_as_number float NULL,                        
#			measurement_datetime datetime NULL,
#			measurement_time varchar(10) NULL,
#			operator_concept_id integer NULL,
#			value_as_concept_id integer NULL,
#			unit_concept_id integer NULL,
#			range_low float NULL,
#			range_high float NULL,
#			provider_id integer NULL,
#			visit_occurrence_id integer NULL,
#			visit_detail_id integer NULL,
#			measurement_source_value varchar(50) NULL,
#			measurement_source_concept_id integer NULL,
#			unit_source_value varchar(50) NULL,
#			unit_source_concept_id integer NULL,
#			value_source_value varchar(50) NULL,
#			measurement_event_id integer NULL,
#			meas_event_field_concept_id integer NULL );

spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
values (
(df_measurement.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tprobson, df_sinasc.tprobson)""") # TPROBSON	Código do Grupo de Robson, gerado pelo sistema
spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
values (
(df_measurement.identity, df_sinasc.identity, 4014304, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.apgar1, df_sinasc.apgar1)""") # APGAR1	Apgar no 1º minuto
spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
values (
(df_measurement.identity, df_sinasc.identity, 4016464, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.apgar5, df_sinasc.apgar5)""") # APGAR5	Apgar no 5º minuto
spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
values (
(df_measurement.identity, df_sinasc.identity, 4264825, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.peso, df_sinasc.peso)""") # PESO	Peso ao nascer em gramas.



#registro observation
#CREATE TABLE observation (
#			observation_id integer NOT NULL,
#			person_id integer NOT NULL,
#			observation_concept_id integer NOT NULL,
#			observation_date date NOT NULL,
#			observation_type_concept_id integer NOT NULL,
#			value_as_number float NULL,
#			value_source_value varchar(50) NULL, 
#			observation_datetime datetime NULL,
#			value_as_string varchar(60) NULL,
#			value_as_concept_id integer NULL,
#			qualifier_concept_id integer NULL,
#			unit_concept_id integer NULL,
#			provider_id integer NULL,
#			visit_occurrence_id integer NULL,
#			visit_detail_id integer NULL,
#			observation_source_value varchar(50) NULL,
#			observation_source_concept_id integer NULL,
#			unit_source_value varchar(50) NULL,
#			qualifier_source_value varchar(50) NULL,
#			observation_event_id integer NULL,
#			obs_event_field_concept_id integer NULL );
# usado type_concept  Government Report 32848

spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, 35810075, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.idademae	 , df_sinasc.idademae	 )""") # IDADEMAE	Idade da mãe
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, 35810331, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.idadepai	 , df_sinasc.idadepai	 )"") # IDADEPAI	Idade do pai
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.qtdfilmort, df_sinasc.qtdfilmort   )""") # QTDFILMORT	Número de filhos mortos
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.qtdfilvivo, df_sinasc.qtdfilvivo   )""") # QTDFILVIVO	Número de filhos vivos
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.qtdgestant, df_sinasc.qtdgestant   )""") # QTDGESTANT	Número de gestações anteriores
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.qtdpartces, df_sinasc.qtdpartces   )""") # QTDPARTCES	Número de partos cesáreos
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.qtdpartnor, df_sinasc.qtdpartnor   )""") # QTDPARTNOR	Número de partos vaginais
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.dtnascmae	, df_sinasc.dtnascmae	 )""") # DTNASCMAE	Data de nascimento da mãe: dd mm aaaa
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.racacormae, df_sinasc.racacormae   )""") # RACACORMAE	1 Tipo de raça e cor da mãe: 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codmunnasc, df_sinasc.codmunnasc   )""") # CODMUNNASC	Código do município de nascimento
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codmunnatu, df_sinasc.codmunnatu   )""") # CODMUNNATU	Código do município de naturalidade da mãe
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codocupmae, df_sinasc.codocupmae   )""") # CODOCUPMAE	Código de ocupação da mãe conforme tabela do CBO (Código Brasileiro de Ocupações).
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codufnatu	, df_sinasc.codufnatu	 )""") # CODUFNATU	Código da UF de naturalidade da mãe
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.dtregcart	, df_sinasc.dtregcart	 )""") # DTREGCART	Data do registro no cartório
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.escmae	 	 , df_sinasc.escmae	 	  )""") # ESCMAE		Escolaridade, em anos de estudo concluídos: 1 – Nenhuma; 2 – 1 a 3 anos; 3 – 4 a 7 anos; 4 – 8 a 11 anos; 5 – 12 e mais; 9 – Ignorado.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.escmae2010, df_sinasc.escmae2010   )""") # ESCMAE2010	Escolaridade 2010. Valores: 0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.escmaeagr1, df_sinasc.escmaeagr1   )""") # ESCMAEAGR1	Escolaridade 2010 agregada. Valores: 00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.estcivmae	, df_sinasc.estcivmae	 )""") # ESTCIVMAE	Situação conjugal da mãe: 1– Solteira; 2– Casada; 3– Viúva; 4– Separada judicialmente/divorciada; 5– União estável; 9– Ignorada.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.naturalmae, df_sinasc.naturalmae   )""") # NATURALMAE	Se a mãe for estrangeira, constará o código do país de nascimento.
spark.sql("""insert into observation (observation_id,person_id ,observation_concept_id ,observation_date ,observation_type_concept_id ,value_as_number,value_source_value)
values (
(df_observation.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.seriescmae, df_sinasc.seriescmae   )""") # SERIESCMAE	Série escolar da mãe. Valores de 1 a 8.




#insert into person_ext
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpdocresp = 1 then 9999999 when df_sinasc.tpdocresp = 2 then when df_sinasc.tpdocresp = 3 then 999999 when df_sinasc.tpdocresp = 4 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpdocresp), # TPDOCRESP	Tipo do documento do responsável. Valores: 1‐CNES; 2‐CRM; 3‐ COREN; 4‐RG; 5‐CPF.
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpfuncresp = 1 then 4000621 when df_sinasc.tpfuncresp = 2 then 32581 when df_sinasc.tpfuncresp = 3 then 40561317 when df_sinasc.tpnascassi = 4 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpfuncresp), # TPFUNCRESP	Tipo de função do responsável pelo preenchimento. Valores: 1– Médico; 2– Enfermeiro; 3– Parteira; 4– Funcionário do cartório; 5– Outros.
#
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.idanomal = 1 then df_sinasc.anomalia when df_sinasc.idanomal = 2 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.idanomal), # IDANOMAL	Anomalia identificada: 1– Sim; 2– Não; 9– Ignorado
