# inicialização
import os
import sys
import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as FSql
from utils import *
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, DoubleType, FloatType
from py4j.java_gateway import java_import
from typing import Tuple
import pandas as pd

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

def loadIdc10(spark: SparkSession, logger: logging.Logger):
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
    estados_cols = ["nome_uf","codigo_uf","uf"]
    df_estados = spark.createDataFrame(data=estados, schema = estados_cols)
    return df_estados

def loadProviderRebios(spark: SparkSession, logger: logging.Logger):
    # load do provider. Serão criados os valores genéricos de entrada. Valores: 1– Médico; 2– Enfermeira/obstetriz; 3– Parteira; 4– Outros; 9– Ignorado
    #CREATE TABLE provider (
    #			provider_id integer NOT NULL,
    #			provider_name varchar(255) NULL,
    #			npi varchar(20) NULL,                    #código cnes dos estabelecimentos de saúde nos EUA
    #			dea varchar(20) NULL,                    #código de registro do agente de saúde nos EUA
    #			specialty_concept_id integer NULL,
    #			care_site_id integer NULL,
    #			year_of_birth integer NULL,
    #			gender_concept_id integer NULL,
    #			provider_source_value varchar(50) NULL,
    #			specialty_source_value varchar(50) NULL,
    #			specialty_source_concept_id integer NULL,
    #			gender_source_value varchar(50) NULL,
    #			gender_source_concept_id integer NULL );

    #registro do provider
    #   rever esse insert para ser provider (df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpnascassi = 1 then 4000621 when df_sinasc.tpnascassi = 2 then 32581 when df_sinasc.tpnascassi = 3 then 40561317 when df_sinasc.tpnascassi = 4 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpnascassi), 
    # # TPNASCASSI	Nascimento foi assistido por? Valores: 1– Médico; 2– Enfermeira/obstetriz; 3– Parteira; 4– Outros; 9– Ignorado

    try:
        spark.sql("""insert into bios.rebios.provider (provider_id, specialty_concept_id, specialty_source_value, specialty_source_concept_id)        values (1L, 4206451L, 'Médico', 1L)""")
        spark.sql("""insert into bios.rebios.provider (provider_id, specialty_concept_id, specialty_source_value, specialty_source_concept_id)        values (2L, 32581L, 'Enfermeira/obstetriz', 2L)""")
        spark.sql("""insert into bios.rebios.provider (provider_id, specialty_concept_id, specialty_source_value, specialty_source_concept_id)        values (3L, 40561317L, 'Parteira', 3L)""")
        spark.sql("""insert into bios.rebios.provider (provider_id, specialty_concept_id, specialty_source_value, specialty_source_concept_id)        values (4L, 3245354L, 'Outros', 4L)""")
        spark.sql("""insert into bios.rebios.provider (provider_id, specialty_concept_id, specialty_source_value, specialty_source_concept_id)        values (5L, 3400510L, 'Ignorado', 9L)""")
    except Exception as e:
        logger.error(f"Error while loading Provider data from DATASUS source to OMOP database: {str(e)}")
        sys.exit(-1)

def loadCareSiteRebios(file_path: str, file_name: str, df_location_cnes: DataFrame, spark: SparkSession, logger: logging.Logger):

#"CO_UNIDADE";"CO_CNES";"NU_CNPJ_MANTENEDORA";"TP_PFPJ";"NIVEL_DEP";"NO_RAZAO_SOCIAL";"NO_FANTASIA";"NO_LOGRADOURO";"NU_ENDERECO";"NO_COMPLEMENTO";"NO_BAIRRO";"CO_CEP";"CO_REGIAO_SAUDE";"CO_MICRO_REGIAO";"CO_DISTRITO_SANITARIO";"CO_DISTRITO_ADMINISTRATIVO";"NU_TELEFONE";"NU_FAX";"NO_EMAIL";"NU_CPF";"NU_CNPJ";"CO_ATIVIDADE";"CO_CLIENTELA";"NU_ALVARA";"DT_EXPEDICAO";"TP_ORGAO_EXPEDIDOR";"DT_VAL_LIC_SANI";"TP_LIC_SANI";"TP_UNIDADE";"CO_TURNO_ATENDIMENTO";"CO_ESTADO_GESTOR";"CO_MUNICIPIO_GESTOR";"TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')";"CO_USUARIO";"CO_CPFDIRETORCLN";"REG_DIRETORCLN";"ST_ADESAO_FILANTROP";"CO_MOTIVO_DESAB";"NO_URL";"NU_LATITUDE";"NU_LONGITUDE";"TO_CHAR(DT_ATU_GEO,'DD/MM/YYYY')";"NO_USUARIO_GEO";"CO_NATUREZA_JUR";"TP_ESTAB_SEMPRE_ABERTO";"ST_GERACREDITO_GERENTE_SGIF";"ST_CONEXAO_INTERNET";"CO_TIPO_UNIDADE";"NO_FANTASIA_ABREV";"TP_GESTAO";"TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')";"CO_TIPO_ESTABELECIMENTO";"CO_ATIVIDADE_PRINCIPAL";"ST_CONTRATO_FORMALIZADO";"CO_TIPO_ABRANGENCIA"
#"2609602569302";"2569302";"10404184000109";"3";"3";"PREFEITURA MUNICIPAL DE OLINDA";"USF BULTRINS MONTE II";"RUA PREFEITO MANOEL REGUEIRA";"540";"";"BULTRINS";"53320460";"001";"";"";"";"(81)34930626";"";"";"";"";"04";"02";"";"";"";"";"";"02";"03";"26";"260960";"28/11/2017";"ANA KARLA";"10258000449";"157002";"";"04";"";"";"";"";"";"1244";"N";"";"S";"";"";"M";"18/06/2003";"";"";"";""
#"2609602571943";"2571943";"10404184000109";"3";"3";"PREFEITURA MUNICIPAL DE OLINDA";"UNIDADE MOVEL";"RUA DO SOL";"311";"";"CARMO";"53120010";"001";"";"02";"";"(81)34294465";"";"";"";"";"04";"01";"";"";"";"";"";"40";"01";"26";"260960";"08/07/2024";"ANA KARLA";"68887302472";"6191";"";"";"";"-8.0105168";"-34.8427588";"06/07/2023";"ANA";"1244";"N";"";"N";"";"";"M";"18/06/2003";"016";"001";"";""
#"2609602344637";"2344637";"10404184000109";"3";"3";"PREFEITURA MUNICIPAL DE OLINDA";"USF ALTO DA MINA";"RUA AVENCA";"49";"";"ALTO DA MINA";"53250441";"001";"";"02";"";"(81)33051133";"";"";"";"";"04";"01";"";"";"";"";"";"02";"03";"26";"260960";"01/07/2024";"ANA KARLA";"03760998445";"322481";"";"";"";"-7.9944275";"-34.8534609";"03/08/2022";"ANA";"1244";"N";"";"S";"";"";"M";"30/10/2001";"001";"012";"";""
#"2609602344696";"2344696";"10404184000109";"3";"3";"PREFEITURA MUNICIPAL DE OLINDA";"USF ILHA DE SANTANA I E II";"RUA DA INTEGRACAO";"S/N";"";"JARDIM ATLANTICO";"53060001";"001";"";"02";"";"(81)34324703";"";"";"";"";"04";"01";"";"";"";"";"";"02";"03";"26";"260960";"01/07/2024";"ANA KARLA";"58346740468";"1399172";"";"";"";"-8.0412002";"-34.879982";"06/07/2023";"ANA";"1244";"N";"";"S";"";"";"M";"30/10/2001";"001";"012";"";""
#"3112002142295";"2142295";"";"3";"1";"FUNDACAO COMUNITARIA DE SAUDE DE CANDEIAS";"HOSPITAL CARLOS CHAGAS";"AVENIDA PEDRO VIEIRA DE AZEVEDO";"687";"";"CENTRO";"37280000";"15";"";"";"";"035-3833.-1285";"";"hospitalcarloschagas@yahoo.com.br";"";"19343383000129";"04";"03";"SRS/VS/DIV/139/2017";"28-set-2017 00:00:00";"1 ";"";"";"05";"06";"31";"311200";"03/04/2024";"ISABEL";"08722114602";"63649";"2";"";"";"-20.767";"-45.276";"30/04/2019";"ADRIANA";"3069";"S";"";"S";"";"";"M";"21/03/2002";"006";"009";"S";""
#"3112602121506";"2121506";"";"3";"1";"LABORATORIO CENTRAL DE CAPINOPOLIS LTDA";"LABORATORIO CENTRAL";"AV 99";"613";"";"CENTRO";"38360000";"026";"";"";"";"(34)32631058";"";"";"";"18587469000134";"04";"03";"055/02/26";"28-mai-2002 00:00:00";"1 ";"";"";"39";"03";"31";"311260";"04/07/2008";"DADS";"";"";"";"02";"";"";"";"";"";"2000";"";"";"";"";"";"D";"12/03/2002";"";"";"";""
#"3112902759861";"2759861";"";"3";"1";"LABORATORIO SANTA HELENA LTDA";"LABORATORIO SANTA HELENA";"AV MANOEL FRANCISCO DE FREITAS";"57";"";"CENTRO";"36925000";"024";"";"";"";"(31)38735235";"";"laboratoriostahelena@ig.com.br";"";"02135527000159";"04";"03";"";"";"";"";"";"39";"03";"31";"311290";"20/06/2023";"SMSCAPUTIRA";"08001522636";"23679";"";"";"";"-20.172";"-42.271";"17/07/2019";"SMSCAPUTIRA";"2062";"N";"";"S";"";"";"M";"12/09/2003";"018";"002";"";""    

#"CO_UNIDADE"
#"CO_CNES"
#"NU_CNPJ_MANTENEDORA"
#"TP_PFPJ"
#"NIVEL_DEP"
#"NO_RAZAO_SOCIAL"
#"NO_FANTASIA"
#"NO_LOGRADOURO"
#"NU_ENDERECO"
#"NO_COMPLEMENTO"
#"NO_BAIRRO"
#"CO_CEP"
#"CO_REGIAO_SAUDE"
#"CO_MICRO_REGIAO"
#"CO_DISTRITO_SANITARIO"
#"CO_DISTRITO_ADMINISTRATIVO"
#"NU_TELEFONE"
#"NU_FAX"
#"NO_EMAIL"
#"NU_CPF"
#"NU_CNPJ"
#"CO_ATIVIDADE"
#"CO_CLIENTELA"
#"NU_ALVARA"
#"DT_EXPEDICAO"
#"TP_ORGAO_EXPEDIDOR"
#"DT_VAL_LIC_SANI"
#"TP_LIC_SANI"
#"TP_UNIDADE"
#"CO_TURNO_ATENDIMENTO"
#"CO_ESTADO_GESTOR"
#"CO_MUNICIPIO_GESTOR"
#"TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')"
#"CO_USUARIO"
#"CO_CPFDIRETORCLN"
#"REG_DIRETORCLN"
#"ST_ADESAO_FILANTROP"
#"CO_MOTIVO_DESAB"
#"NO_URL"
#"NU_LATITUDE"
#"NU_LONGITUDE"
#"TO_CHAR(DT_ATU_GEO,'DD/MM/YYYY')"
#"NO_USUARIO_GEO"
#"CO_NATUREZA_JUR"
#"TP_ESTAB_SEMPRE_ABERTO"
#"ST_GERACREDITO_GERENTE_SGIF"
#"ST_CONEXAO_INTERNET"
#"CO_TIPO_UNIDADE"
#"NO_FANTASIA_ABREV"
#"TP_GESTAO"
#"TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')"
#"CO_TIPO_ESTABELECIMENTO"
#"CO_ATIVIDADE_PRINCIPAL"
#"ST_CONTRATO_FORMALIZADO"
#"CO_TIPO_ABRANGENCIA"

    # Essa é a config para o registro na location do endereço de um care_site do cnes
    #CREATE TABLE location (
    #			location_id integer ,                        PK_gerada pelo ETL # por ser o endereço de um hospital, essa PK será usada como fk no care_site
    #			address_1 varchar(50) NULL,                  NO_LOGRADOURO + NU_ENDERECO + NO_COMPLEMENTO 
    #			address_2 varchar(50) NULL,                  NO_BAIRRO
    #			city varchar(50) NULL,                       CO_MUNICIPIO_GESTOR   # se necessário, com essa coluna faz join para obter os dados do município desse endereço usando a coluna location.location_source_value de um município.
    #			state varchar(2) NULL,                       adotar nesse campo a sigla da UF para simplificar a busca
    #			zip varchar(9) NULL,                         CO_CEP
    #			county varchar(20) NULL,
    #			location_source_value varchar(50) NULL,      CO_CNES # essa coluna é para permitir buscar a PK do location de um hospital ao armazenar ele na tabela care_site
    #			country_concept_id integer NULL,             4075645L  código do Brasil
    #			country_source_value varchar(80) NULL,       'Brasil'
    #			latitude float NULL,                         NU_LATITUDE p/ estabelecimento de saúde 
    #			longitude float NULL );                      NU_LONGITUDE p/ estabelecimento de saúde 

    #CREATE TABLE care_site (
    #			care_site_id integer ,                                 PK gerada pelo ETL
    #			care_site_name varchar(255) NULL,                      NO_RAZAO_SOCIAL
    #			place_of_service_concept_id integer NULL,
    #			location_id integer NULL,                              PK da location
    #			care_site_source_value varchar(50) NULL,               CO_CNES
    #			place_of_service_source_value varchar(50) NULL );

    logger.info("Loading of list of care site records on table CARE_SITE started.")

    #esses dois estabelecimentos são valores pré-definidos próprios do Climaterna
    #não existe registro em location correspondente
    spark.sql("""insert into bios.rebios.care_site(care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value)
    values (3L, 'Nascimento no Domicílio', 43021744L, null, 3L, 'Domicílio')""")   #43021744 Born at home
    spark.sql("""insert into bios.rebios.care_site(care_site_id,care_site_name,place_of_service_concept_id,location_id,care_site_source_value,place_of_service_source_value)
    values (4L, "Nascimento em Outros Locais", 45881550L, null, 4L, "Outros")""") #45881550 Place of birth unknown

    # os estabelecimentos de saúde serão cadastrados em location e repetidos como care_site por falta de detalhes no SIM/SINASC. O care_site terá FK do location.
    # A partir desse ponto acontece a inserção em lote dos estabelecimentos de saúde a partir da base CNES
    # o dataframe de entrada contém os dados do arquivo de cnes e os dados inseridos na tabela location

    df_load_schema = StructType([ \
    StructField("care_site_id", LongType(), False), \
    StructField("care_site_name", StringType(), True), \
    StructField("place_of_service_concept_id", LongType(), True), \
    StructField("location_id", LongType(), True), \
    StructField("care_site_source_value", StringType(), True), \
    StructField("place_of_service_source_value", StringType(), True) \
   ])
    
    if df_location_cnes.count() > 0:
        df_care_site = spark.createDataFrame(df_location_cnes.select( \
        FSql.lit(0).cast(LongType()).alias('care_site_id'), \
        df_location_cnes.NO_RAZAO_SOCIAL.alias('care_site_name'), \
   		FSql.lit(None).cast(StringType()).alias('place_of_service_concept_id'), \
        df_location_cnes.location_id.alias('location_id'), \
        df_location_cnes.CO_CNES.alias('care_site_source_value'), \
   		FSql.lit(None).cast(StringType()).alias('place_of_service_source_value') \
        ).rdd, df_load_schema)
        
        if df_care_site.count() > 0:
            #obtem o max da tabela para usar na inserção de novos registros
            count_max_care_site_df = spark.sql("SELECT greatest(max(care_site_id),0) + 1 AS max_care_site FROM bios.rebios.care_site")
            count_max_care_site = count_max_care_site_df.first().max_care_site
            #geração dos id's únicos nos dados de entrada. O valor inicial é 1.
            # a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
            # do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
            df_care_site = df_care_site.withColumn("care_site_id", monotonically_increasing_id())
            #sincroniza os id's gerados com o max(person_id) existente no banco de dados
            df_care_site = df_care_site.withColumn("care_site_id", df_care_site["care_site_id"] + count_max_care_site)

            # the show command below is to force the dataframe to be checked against its structure field. The error trap is outside this routine.
            df_care_site.show()   
            df_care_site.writeTo("bios.rebios.care_site").append()
            logger.info("Care sites data succesfully written to table CARE_SITE")

def loadLocationCnesRebios(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
    #load dos estabelecimentos de saúde CNES. Cada establecimento de saúde é uma location que se repete no care_site visto que não temos dados das divisões/unidades dos estabelecimentos de saúde.
    #"1200452000725";"2000725";"04034526000143";"3";"3";"SECRETARIA DE ESTADO DE SAUDE";"HOSPITAL DR ARY RODRIGUES";"AV SENADOR EDUARDO ASSMAR";"153";"";"COHAB";"69925000";"001";"";"";"";"(68)3232 2956";"";"hospitalaryrodrigues201705@gmail.com";"";"04034526001115";"04";"03";"";"";"";"";"";"05";"06";"12";"120045";"27/03/2024";"SCNES";"63786311234";"";"";"";"";"-10.151";"-67.736";"11/07/2019";"SCNES";"1023";"S";"";"S";"";"";"E";"30/10/2001";"006";"009";"";""

    # Tendo como source o CSV tbEstabelecimento999999.csv contendo os estabelecimentos de saúde. Um registro equivalente será adicionado na tabela caresite na rotina seguinte
    # Essa é a config para o registro na location do endereço de um care_site do cnes
    #CREATE TABLE location (
    #			location_id integer ,                        PK_gerada pelo ETL # por ser o endereço de um hospital, essa PK será usada como fk no care_site
    #			address_1 varchar(50) NULL,                  NO_LOGRADOURO + NU_ENDERECO + NO_COMPLEMENTO 
    #			address_2 varchar(50) NULL,                  NO_BAIRRO
    #			city varchar(50) NULL,                       CO_MUNICIPIO_GESTOR   # se necessário, com essa coluna faz join para obter os dados do município desse endereço usando a coluna location.location_source_value de um município.
    #			state varchar(2) NULL,                       adotar nesse campo a sigla da UF para simplificar a busca
    #			zip varchar(9) NULL,                         CO_CEP
    #			county varchar(20) NULL,
    #			location_source_value varchar(50) NULL,      CO_CNES # essa coluna é para permitir buscar a PK do location de um hospital ao armazenar ele na tabela care_site
    #			country_concept_id integer NULL,             4075645L  código do Brasil
    #			country_source_value varchar(80) NULL,       'Brasil'
    #			latitude float NULL,                         NU_LATITUDE p/ estabelecimento de saúde 
    #			longitude float NULL );                      NU_LONGITUDE p/ estabelecimento de saúde 


    # Essa é a config para o registro na location de um município.
    #CREATE TABLE location (
    #			location_id integer ,                        PK_gerada pelo ETL
    #			city varchar(50) NULL,                       NOME_MUNICIPIO
    #			state varchar(2) NULL,                       nome_uf
    #			county varchar(20) NULL,                     codigo_uf
    #			location_source_value varchar(50) NULL,      GEOCODIGO_MUNICIPIO
    #			country_concept_id integer NULL,             4075645L  código do Brasil
    #			country_source_value varchar(80) NULL,       'Brasil'
    #			latitude float NULL,                         latitude do município p/ municípios
    #			longitude float NULL );                      longitude do município p/ municípios

    #"CO_UNIDADE"
    #"CO_CNES"
    #"NU_CNPJ_MANTENEDORA"
    #"TP_PFPJ"
    #"NIVEL_DEP"
    #"NO_RAZAO_SOCIAL"
    #"NO_FANTASIA"
    #"NO_LOGRADOURO"
    #"NU_ENDERECO"
    #"NO_COMPLEMENTO"
    #"NO_BAIRRO"
    #"CO_CEP"
    #"CO_REGIAO_SAUDE"
    #"CO_MICRO_REGIAO"
    #"CO_DISTRITO_SANITARIO"
    #"CO_DISTRITO_ADMINISTRATIVO"
    #"NU_TELEFONE"
    #"NU_FAX"
    #"NO_EMAIL"
    #"NU_CPF"
    #"NU_CNPJ"
    #"CO_ATIVIDADE"
    #"CO_CLIENTELA"
    #"NU_ALVARA"
    #"DT_EXPEDICAO"
    #"TP_ORGAO_EXPEDIDOR"
    #"DT_VAL_LIC_SANI"
    #"TP_LIC_SANI"
    #"TP_UNIDADE"
    #"CO_TURNO_ATENDIMENTO"
    #"CO_ESTADO_GESTOR"
    #"CO_MUNICIPIO_GESTOR"
    #"TO_CHAR(DT_ATUALIZACAO,'DD/MM/YYYY')"
    #"CO_USUARIO"
    #"CO_CPFDIRETORCLN"
    #"REG_DIRETORCLN"
    #"ST_ADESAO_FILANTROP"
    #"CO_MOTIVO_DESAB"
    #"NO_URL"
    #"NU_LATITUDE"
    #"NU_LONGITUDE"
    #"TO_CHAR(DT_ATU_GEO,'DD/MM/YYYY')"
    #"NO_USUARIO_GEO"
    #"CO_NATUREZA_JUR"
    #"TP_ESTAB_SEMPRE_ABERTO"
    #"ST_GERACREDITO_GERENTE_SGIF"
    #"ST_CONEXAO_INTERNET"
    #"CO_TIPO_UNIDADE"
    #"NO_FANTASIA_ABREV"
    #"TP_GESTAO"
    #"TO_CHAR(DT_ATUALIZACAO_ORIGEM,'DD/MM/YYYY')"
    #"CO_TIPO_ESTABELECIMENTO"
    #"CO_ATIVIDADE_PRINCIPAL"
    #"ST_CONTRATO_FORMALIZADO"
    #"CO_TIPO_ABRANGENCIA"

    # Estrutura SINASC até 2019
    ################################
    # inserção do location de cada município de entrada como sendo o endereço da person. 
    # Linhas de location adicionais serão criadas para conter os demais municípios de entrada, como CODMUNCART, CODMUNNASC, CODMUNNATU, considerando todas as colunas de município.
    # Foi adotado dentro do projeto que o município de nascimento será o de endereço da mãe (CODMUNRES).
    ################################

    logger.info("Loading of list of care site locations on table LOCATION started.")

    df_load_schema = StructType([ \
    StructField("location_id", LongType(), False), \
    StructField("address_1", StringType(), True), \
    StructField("address_2", StringType(), True), \
    StructField("city", StringType(), True), \
    StructField("state", StringType(), True), \
    StructField("zip", StringType(), True), \
    StructField("location_source_value", StringType(), True), \
    StructField("country_concept_id", LongType(), True), \
    StructField("country_source_value", StringType(), True), \
    StructField("latitude", FloatType(), True), \
    StructField("longitude", FloatType(), True), \
    StructField("county", StringType(), True) \
   ])
    
    # essa inicialização é para garantir algum retorno ao término da rotina
    df_location = spark.createDataFrame([], df_load_schema)

    df_load = spark.read.csv(os.path.join(file_path, file_name), sep=";", header=True, inferSchema=True)
    if df_load.count() > 0:
        df_location = spark.createDataFrame(df_load.select( \
        FSql.lit(0).cast(LongType()).alias('location_id'), \
        FSql.concat(df_load.NO_LOGRADOURO, FSql.lit(" "), df_load.NU_ENDERECO, FSql.lit(" "), df_load.NO_COMPLEMENTO).alias('address_1'), \
        df_load.NO_BAIRRO.alias('address_2'), \
        df_load.CO_MUNICIPIO_GESTOR.alias('city'), \
        df_load.CO_ESTADO_GESTOR.alias('state'), \
        df_load.CO_CEP.alias('zip'), \
        df_load.CO_CNES.alias('location_source_value'), \
        FSql.lit(4075645).cast(LongType()).alias('country_concept_id'), \
        FSql.lit('Brasil').alias('country_source_value'), \
        df_load.NU_LATITUDE.cast(FloatType()).alias('latitude'), \
        df_load.NU_LONGITUDE.cast(FloatType()).alias('longitude'), \
        FSql.lit(None).cast(StringType()).alias('county') \
        ).rdd, df_load_schema)
        
        if df_location.count() > 0:
            #obtem o max da tabela para usar na inserção de novos registros
            count_max_location_df = spark.sql("SELECT greatest(max(location_id),0) + 1 AS max_location FROM bios.rebios.location")
            count_max_location = count_max_location_df.first().max_location
            #geração dos id's únicos nos dados de entrada. O valor inicial é 1.
            # a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
            # do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
            df_location = df_location.withColumn("location_id", monotonically_increasing_id())
            #sincroniza os id's gerados com o max(person_id) existente no banco de dados
            df_location = df_location.withColumn("location_id", df_location["location_id"] + count_max_location)

            # the show command below is to force the dataframe to be checked against its structure field. The error trap is outside this routine.
            df_location.show()   
            df_location.writeTo("bios.rebios.location").append()
            # a junção dos dois df é para ser usado na rotina que insere o care_site.
            # o df vai completo com a PK do location para ser usada como fk no care_site.
            df_location = (df_location.join(df_load, [df_location.location_source_value == df_load.CO_CNES], 'inner'))
            logger.info("Cities data succesully written to table LOCATION")
    return df_location

def loadLocationCityRebios(file_path: str, file_name: str, spark: SparkSession, logger: logging.Logger):
    # Tendo como source o CSV de municípios RELATORIO_DTB_BRASIL_MUNICIPIO.xls. 
    #CREATE TABLE location (
    #			location_id integer ,                        PK_gerada pelo ETL
    #			city varchar(50) NULL,                       NOME_MUNICIPIO
    #			state varchar(2) NULL,                       nome_uf
    #			county varchar(20) NULL,                     codigo_uf
    #			location_source_value varchar(50) NULL,      GEOCODIGO_MUNICIPIO
    #			country_concept_id integer NULL,             4075645L  código do Brasil
    #			country_source_value varchar(80) NULL,       'Brasil'
    #			latitude float NULL,                         latitude do município p/ municípios
    #			longitude float NULL );                      longitude do município p/ municípios

#Arquivo do IBGE: 
#OID_;
#nome;
#geocodigo;
#Longitude;
#Latitude

    # Estrutura SINASC até 2019
    ################################
    # inserção do location de cada município de entrada como sendo o endereço da person. 
    # Linhas de location adicionais serão criadas para conter os demais municípios de entrada, como CODMUNCART, CODMUNNASC, CODMUNNATU, considerando todas as colunas de município.
    # Foi adotado dentro do projeto que o município de nascimento será o de endereço da mãe (CODMUNRES).
    ################################

    df_states = loadStates(spark, logger)

    logger.info("Loading of list of cities on table LOCATION started.")
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
    
    df_load = df_load.withColumn("Latitude", FSql.regexp_replace("Latitude", "," , "."))
    df_load = df_load.withColumn("Longitude", FSql.regexp_replace("Longitude", "," , "."))

    if df_load.count() > 0:
        df_location = spark.createDataFrame(df_load.select(\
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
            #obtem o max da tabela para usar na inserção de novos registros
            count_max_location_df = spark.sql("SELECT greatest(max(location_id),0) + 1 AS max_location FROM bios.rebios.location")
            count_max_location = count_max_location_df.first().max_location
            #geração dos id's únicos nos dados de entrada. O valor inicial é 1.
            # a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
            # do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
            df_location = df_location.withColumn("location_id", monotonically_increasing_id())
            #sincroniza os id's gerados com o max(person_id) existente no banco de dados
            df_location = df_location.withColumn("location_id", df_location["location_id"] + count_max_location)

            # the show command below is to force the dataframe to be checked against its structure field. The error trap is outside this routine.
            df_location.show()   
            df_location.writeTo("bios.rebios.location").append()
            logger.info("City data succesully written to table LOCATION")
            df_location.unpersist()

