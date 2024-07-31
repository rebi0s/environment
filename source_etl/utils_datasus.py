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

def loadCities(spark: SparkSession, logger: logging.Logger):
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

def loadTypeOfUnit(spark: SparkSession, logger: logging.Logger):
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
    return df_cnes_tpunid

def loadCid10(spark: SparkSession, logger: logging.Logger):
    #load do cid10 com vocabulário do omop
    cid10 = [
    ('R19.3',45606798)    
    ]
    cid10_cols = ["codigo_cid10", "conceptid"]
    df_cid10 = spark.createDataFrame(data=cid10, schema = cid10_cols)

def loadStates(spark: SparkSession, logger: logging.Logger):
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

def loadProviderRebios(spark: SparkSession, logger: logging.Logger):
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

def loadCareSiteRebios(spark: SparkSession, logger: logging.Logger):
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
    df_sinasc.codestab,
    null
    )""")

def loadLocationRebios(spark: SparkSession, logger: logging.Logger):
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

    # Estrutura SINASC até 2019
    ################################
    # inserção do location de cada município de entrada como sendo o endereço da person. 
    # Linhas de location adicionais serão criadas para conter os demais municípios de entrada, como CODMUNCART, CODMUNNASC, CODMUNNATU, considerando todas as colunas de município.
    # Foi adotado dentro do projeto que o município de nascimento será o de endereço da mãe (CODMUNRES).
    ################################

    #por ser PK será utilizado o código completo do munícipio com os dígitos do estado do início do código. o último dígito é o código verificador. apenas o código do munícipio gera repetição.
    #código do Brazil obtido no Athena do vocabulario SNOMED. Vai ser necessário um tratamento para os casos envolvendo estrangeiros. No sinasc o campo CODPAISRES vem com 1 para os brasileiros.
    spark.sql("""insert into location ( 
                location_id  , 
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
    df_cnes = (df_cnes.join(df_cnes_tpunid, on=['df_cnes.tpunid == df_cnes_tpunid.codigo'])) # retorna o conceptid do tipo da unidade 

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
                location_id  ,
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

