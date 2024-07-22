# inicialização
import os
import sys
import logging
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql import functions as FSql
from utils import *

logger = configLogger('main');
# o nível de log efetivamente registrado depende da variável de ambiente utilizada abaixo.
logHandler = addLogHandler(logger, os.getenv("CTRNA_LOG_LEVEL", "INFO"))

logger.info('Log configuration done')

spark = initSpark()

# Mensagens de log
logger.info('Spark session started.')

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


####################################################################
##  Leitura do arquivo de entrada (source)                        ##
####################################################################

# APGAR1	Apgar no 1º minuto
# APGAR5	Apgar no 5º minuto
# CODANOMAL	Código da anomalia (CID 10)
# CODCART	Código do cartório
# CODESTAB	Código do estabelecimento de saúde onde ocorreu o nascimento
# CODMUNCART	Código do município do cartório
# CODMUNNASC	Código do município de nascimento
# CODMUNNATU	Código do município de naturalidade da mãe
# CODMUNRES	Código do município de residência
# CODOCUPMAE	Código de ocupação da mãe conforme tabela do CBO (Código Brasileiro de Ocupações).
# CODPAISRES	Código do país de residência
# CODUFNATU	Código da UF de naturalidade da mãe
# CONSPRENAT	Número de consultas pré‐natal
# CONSULTAS	Número de consultas de pré‐natal. Valores: 1– Nenhuma; 2– de 1 a 3; 3– de 4 a 6; 4– 7 e mais; 9– Ignorado.
# contador	Contador indo de 1 até n, sendo n o número de observações 
# DIFDATA	Diferença entre a data de óbito e data do recebimento original da DO ([DTNASC] – [DTRECORIG])
# DTCADASTRO	Data do cadastro da DN no sistema
# DTDECLARAC	Data da declaração: dd mm aaaa
# DTNASC	Data de nascimento: dd mm aaaa
# DTNASCMAE	Data de nascimento da mãe: dd mm aaaa
# DTRECEBIM	Data do último recebimento do lote, dada pelo Sisnet.
# DTRECORIG	Data do 1º recebimento do lote, dada pelo Sisnet.
# DTRECORIGA	
# DTREGCART	Data do registro no cartório
# DTULTMENST	Data da última menstruação (DUM): dd mm aaaa
# ESCMAE	Escolaridade, em anos de estudo concluídos: 1 – Nenhuma; 2 – 1 a 3 anos; 3 – 4 a 7 anos; 4 – 8 a 11 anos; 5 – 12 e mais; 9 – Ignorado.
# ESCMAE2010	Escolaridade 2010. Valores: 0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado.
# ESCMAEAGR1	Escolaridade 2010 agregada. Valores: 00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico.
# ESTCIVMAE	Situação conjugal da mãe: 1– Solteira; 2– Casada; 3– Viúva; 4– Separada judicialmente/divorciada; 5– União estável; 9– Ignorada.
# GESTACAO	Semanas de gestação: 1– Menos de 22 semanas; 2– 22 a 27 semanas; 3– 28 a 31 semanas; 4– 32 a 36 semanas; 5– 37 a 41 semanas; 6– 42 semanas e mais; 9– Ignorado.
# GRAVIDEZ	Tipo de gravidez: 1– Única; 2– Dupla; 3– Tripla ou mais; 9– Ignorado.
# HORANASC	Horário de nascimento
# IDADEMAE	Idade da mãe
# IDADEPAI	Idade do pai
# IDANOMAL	Anomalia identificada: 1– Sim; 2– Não; 9– Ignorado
# KOTELCHUCK	1 Não fez pré-natal (Campo33=0); 2 Inadequado (Campo34>3 ou Campo34<=3 e Campo33<3); 3 Intermediário (Campo34<=3 e Campo33 entre 3 e 5); 4 Adequado (Campo34<=3 e Campo33=6); 5 Mais que adequado (Campo34<=3 e Campo33>=7); 6 Não Classificados (campos 33 ou 34, Nulo ou Ign)
# LOCNASC	Local de nascimento: 1 – Hospital; 2 – Outros estabelecimentos de saúde; 3 – Domicílio; 4 – Outros.
# MESPRENAT	Mês de gestação em que iniciou o pré‐natal
# NATURALMAE	Se a mãe for estrangeira, constará o código do país de nascimento.
# NUMEROLOTE	Número do lote
# NUMREGCART	Número do registro civil (cartório)
# ORIGEM	
# PARIDADE	0-nulípara; 1 multipara; 9- ignorado
# PARTO	Tipo de parto: 1– Vaginal; 2– Cesário; 9– Ignorado
# PESO	Peso ao nascer em gramas.
# QTDFILMORT	Número de filhos mortos
# QTDFILVIVO	Número de filhos vivos
# QTDGESTANT	Número de gestações anteriores
# QTDPARTCES	Número de partos cesáreos
# QTDPARTNOR	Número de partos vaginais
# RACACOR	Tipo de raça e cor do nascido: 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
# RACACOR_RN	Tipo de raça e cor do nascido: 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
# RACACORMAE	1 Tipo de raça e cor da mãe: 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
# RACACORN	Tipo de raça e cor do nascido: 1– Branca; 2– Preta; 3– Amarela; 4– Parda; 5– Indígena.
# SEMAGESTAC	Número de semanas de gestação.
# SERIESCMAE	Série escolar da mãe. Valores de 1 a 8.
# SEXO	Sexo: M – Masculino; F – Feminino; I – ignorado
# STCESPARTO	Cesárea ocorreu antes do trabalho de parto iniciar? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
# STDNEPIDEM	Status de DN Epidemiológica. Valores: 1 – SIM; 0 – NÃO.
# STDNNOVA	Status de DN Nova. Valores: 1 – SIM; 0 – NÃO.
# STTRABPART	Trabalho de parto induzido? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
# TPAPRESENT	Tipo de apresentação do RN. Valores: 1– Cefálico; 2– Pélvica ou podálica; 3– Transversa; 9– Ignorado.
# TPDOCRESP	Tipo do documento do responsável. Valores: 1‐CNES; 2‐CRM; 3‐ COREN; 4‐RG; 5‐CPF.
# TPFUNCRESP	Tipo de função do responsável pelo preenchimento. Valores: 1– Médico; 2– Enfermeiro; 3– Parteira; 4– Funcionário do cartório; 5– Outros.
# TPMETESTIM	Método para estimar utilizado. Valores: 1– Exame físico; 2– Outro método; 9– Ignorado.
# TPNASCASSI	Nascimento foi assistido por? Valores: 1– Médico; 2– Enfermeira/obstetriz; 3– Parteira; 4– Outros; 9– Ignorado
# TPROBSON	Código do Grupo de Robson, gerado pelo sistema
# VERSAOSIST	Versão do sistema

#carga dos dados do parquet do SINASC
source_path = os.getenv("CTRNA_SOURCE_SINASC_PATH","/home/warehouse/")
arquivo_entrada = "sinasc_2010_2022.parquet"

# leitura do sinasc original em formato parquet
if not os.path.isfile(os.path.join(source_path, arquivo_entrada)):
        logger.info("Arquivo SINASC não localizado. Carga interrompida.")
        sys.exit(0)

df_sinasc = spark.read.parquet(os.path.join(source_path, arquivo_entrada))

####################################################################
##  Carrega em memória os cadastros                               ##
####################################################################

# Table location 
# location_id  ,
# address_1  ,
# address_2  ,
# city  ,
# state ,
# zip ,
# county  ,
# location_source_value  ,
# country_concept_id  ,
# country_source_value ,
# latitude,
# longitude)

# df_teste = spark.sql(f"SELECT * FROM {CATALOG_NAME}.{db.name}.{table.name} LIMIT 5")


#df_sinasc = spark.read.format("CSV").options(header=True, inferSchema=True).load("/home/src/etl/SINASC_REGISTRO_LAIS.csv")

# dataframe com todos os registros de location
df_location = spark.read.format("iceberg").load(f"bios.location")
# dataframe com todos os registros de care_site
df_care_site = spark.read.format("iceberg").load(f"bios.care_site")
# dataframe com todos os registros de concept
df_concept = spark.read.format("iceberg").load(f"bios.concept")

#obtem o max person_id para usar na inserção de novos registros
count_max_person_df = spark.sql("SELECT greatest(max(person_id),0) + 1 AS max_person FROM bios.person")
count_max_person = count_max_person_df.first().max_person
#geração dos id's únicos nos dados de entrada. O valor inicial é 0.
# a função monotonically_increasing_id() gera números incrementais com a garantia de ser sempre maior que os existentes.
df_sinasc = df_sinasc.withColumn("person_id", monotonically_increasing_id())
#sincroniza os id's gerados com o max(person_id) existente no banco de dados atualizando os registros no df antes de escrever no banco
df_sinasc = df_sinasc.withColumn("person_id", df_sinasc["person_id"] + count_max_person)

# esses df's poderão conter valores nulos para município e estabelecimento de saúde, caso não haja cadastro.
# a partir da coluna person_id, os registros de entrada se tornam unicamente identificados.
# left outer join entre sinasc e location para associar dados de município
df_sinasc_location = (df_sinasc.join(df_location, on=['df_sinasc.CODMUNRES == df_location.location_id'], how='left'))
# left outer join entre sinasc e care site para associar dados de estabelecimento de saúde
df_sinasc_cnes = (df_sinasc.join(df_care_site, on=['df_sinasc.CODESTAB == df_care_site.care_site_source_value'], how='left'))


#####  CRIAR O DF PARA CONTENDO O PERSON_ID E O CID10 CORRESPONDENTE
# left outer join entre sinasc e vocabulário para associar dados de 
df_sinasc_cid10 = (df_sinasc.join(df_concept, on=['df_sinasc.codmunres == df_concept.location_id'], how='left'))

# tratamento para resolver a falta de FK's antes da inserção no banco
# inserir novos municípios 

# inserir novos estabelecimentos de saúde

# *************************************************************
#  PERSON - Persistência dos dados 
# *************************************************************
# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_person_schema = StructType([ \
    StructField("person_id", LongType(), False), \
    StructField("gender_concept_id", LongType(), False), \
    StructField("year_of_birth", IntegerType(), False), \
    StructField("month_of_birth", IntegerType(), True), \
    StructField("day_of_birth", IntegerType(), True), \
    StructField("birth_datetime", TimestampType(), True), \
    StructField("race_concept_id", LongType(), False), \
    StructField("ethnicity_concept_id", LongType(), False), \
    StructField("location_id", LongType(), True), \
    StructField("provider_id", LongType(), True), \
    StructField("care_site_id", LongType(), True), \
    StructField("person_source_value", StringType(), True), \
    StructField("gender_source_value", StringType(), True), \
    StructField("gender_source_concept_id", LongType(), True), \
    StructField("race_source_value", StringType(), True), \
    StructField("race_source_concept_id", LongType(), True), \
    StructField("ethnicity_source_value", StringType(), True), \
    StructField("ethnicity_source_concept_id", LongType(), True) \
])

df_person = spark.createDataFrame(df_sinasc.select( \
df_sinasc.person_id, \
FSql.when(df_sinasc['SEXO'] == 'M', 8507).FSql.when(df_sinasc['SEXO'] == 'F', 8532).FSql.when(df_sinasc['SEXO'] == '1', 8507).FSql.when(df_sinasc['SEXO'] == '2', 8532).otherwise(8551).alias('gender_concept_id'), \
year(make_date(substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 5, 4), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 3, 2), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 1, 2))).alias("year_of_birth"), \
month(make_date(substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 5, 4), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 3, 2), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 1, 2))).alias("month_of_birth"), \
dayofmonth(make_date(substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 5, 4), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 3, 2), substring(FSql.lpad(df_sinasc.DTNASC,8,'0'), 1, 2))).alias("day_of_birth"), \
to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias('birth_timestamp'), \
FSql.when(df_sinasc['RACACOR'] == 1, 3212942).FSql.when(df_sinasc['RACACOR'] == 2, 3213733).FSql.when(df_sinasc['RACACOR'] == 3, 3213498).FSql.when(df_sinasc['RACACOR'] == 4, 3213487).otherwise(3213694).alias('race_concept_id'),  \
FSql.lit(38003563).alias('ethnicity_concept_id'), \
df_sinasc.CODMUNRES.alias('location_id'), \
FSql.lit(None).cast(StringType()).alias('provider_id'), \
df_sinasc.CODESTAB.alias('care_site_id'), \
FSql.lit(None).cast(StringType()).alias('person_source_value'), \
df_sinasc.SEXO.alias('gender_source_value'),
FSql.lit(None).cast(StringType()).alias('gender_source_concept_id'), \
df_sinasc.RACACOR.alias('race_source_value'),  
FSql.lit(None).cast(StringType()).alias('race_source_concept_id'), \
FSql.lit(None).cast(StringType()).alias('ethnicity_source_value'), \
FSql.lit(None).cast(StringType()).alias('ethnicity_source_concept_id') \
).rdd, \
df_person_schema)

if df_person.count() > 0:
# Persistindo os dados de person no banco.
	df_person.writeTo("bios.person").append()
else:
	exit()
# *************************************************************
#  OBSERVATION_PERIOD - Persistência dos dados 
# *************************************************************

# |-- observation_period_id: long (nullable = false)
# |-- person_id: long (nullable = false)
# |-- observation_period_start_date: date (nullable = false)
# |-- observation_period_end_date: timestamp (nullable = false)
# |-- period_type_concept_id: long (nullable = false)

# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_obs_period_schema = StructType([ \
    StructField("observation_period_id", LongType(), False), \
    StructField("person_id", LongType(), False), \
    StructField("observation_period_start_date", DateType(), False), \
    StructField("observation_period_end_date", TimestampType(), False), \
    StructField("period_type_concept_id", LongType(), False) \
])

# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_obs_period=spark.createDataFrame(df_sinasc.select(\
                      FSql.lit(0).cast(LongType()).alias('observation_period_id'), \
                      df_sinasc.person_id.alias('person_id'), \
                      to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias("observation_period_start_date"), \
                      to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias('observation_period_end_date'), \
                      FSql.lit(4193440).alias('period_type_concept_id')).rdd, \
                      df_obs_period_schema)
if df_obs_period.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_obs_period_df = spark.sql("SELECT greatest(max(observation_period_id),0) + 1 AS max_obs_period FROM bios.observation_period")
	count_max_obs_period = count_max_obs_period_df.first().max_obs_period
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_obs_period = df_obs_period.withColumn("observation_period_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_obs_period = df_obs_period.withColumn("observation_period_id", df_obs_period["observation_period_id"] + count_max_obs_period)
	# persistindo os dados de observation_period no banco.
	df_obs_period.writeTo("bios.observation_period").append()



####################################################################
##  Persistir os dados no Climaterna com as consistências feitas  ##
####################################################################

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

#spark.sql("""insert into person 
#(
#			person_id  ,
#			gender_concept_id  ,
#			year_of_birth  ,
#			month_of_birth  ,
#			day_of_birth  ,
#			birth_datetime  ,
#			race_concept_id  ,
#			ethnicity_concept_id  ,
#			location_id  ,   
#			provider_id  ,   
#			care_site_id  ,  
#			person_source_value ,
#			gender_source_value ,
#			gender_source_concept_id  ,
#			race_source_value ,
#			race_source_concept_id  ,
#			ethnicity_source_value ,
#			ethnicity_source_concept_id   )
#values
#(
#df_sinasc.identity ,  
#case when df_sinasc.sexo = 'M' then 8507 when df_sinasc.sexo = 'F' then 8532 else 8551 end,  
#year(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
#month(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
#day(makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2))) ,
#make_timestamp(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2), substr(df_sinasc.horanasc,1,2),  substr(df_sinasc.horanasc,3)) ,
#case when df_sinasc.racacor = 1 then 3212942 when df_sinasc.racacor = 2 then 3213733 when df_sinasc.racacor = 3 then 3213498 when df_sinasc.racacor = 4 then 3213487 else 3213694 end,  
#38003563, 
#(select location_id from location where location_source_value = df_sinasc.codmunres), 
#NULL, #provider é nulo porque ele será vinculado a ocorrência do parto e não a pessoa
#(select care_site_id from care_site where care_site_source_value = replace(df_sinasc.codestab,'.')),
#NULL, 
#df_sinasc.sexo,
#NULL,
#df_sinasc.racacor,  
#NULL,
#NULL, 
#NULL)""")
#

#registro do observation_period do parto
#CREATE TABLE observation_period (
#			observation_period_id integer ,
#			person_id integer ,
#			observation_period_start_date date ,
#			observation_period_end_date date ,
#			period_type_concept_id integer  );

#spark.sql("""insert into observation_period (
#			observation_period_id,
#			person_id integer,
#			observation_period_start_date,
#			observation_period_end_date,
#			period_type_concept_id)
#values
#(df_sinasc_obs.identity,
# df_sinasc.identity,
# makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)),
# makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)),
# 4193440  #notification of Birth obtido no vocabulario do Athena
#)""")

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


# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela condition_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: STDNEPIDEM
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.stdnepidem = 1 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.stdnepidem)""") # STDNEPIDEM	Status de DN Epidemiológica. Valores: 1 – SIM; 0 – NÃO.

# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_cond_occur_schema = StructType([ \
StructField("condition_occurrence_id", LongType(), False), \
StructField("person_id", LongType(), False), \
StructField("condition_concept_id", LongType(), False), \
StructField("condition_start_date", DateType(), False), \
StructField("condition_end_date", DateType(), True), \
StructField("condition_type_concept_id", LongType(), False), \
StructField("condition_source_value", StringType(), True) \
])

# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['STDNEPIDEM'] == '1', 999999).otherwise(999998).alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.STDNEPIDEM.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: TPAPRESENT
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpapresent = 1 then 9999999 when df_sinasc.tpapresent = 2 then 9999999 when df_sinasc.tpapresent = 3 then 4218938 else 9999999 , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpapresent)""") # TPAPRESENT	Tipo de apresentação do RN. Valores: 1– Cefálico; 2– Pélvica ou podálica; 3– Transversa; 9– Ignorado.

# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['TPAPRESENT'] == '1', 999999).FSql.when(df_sinasc['TPAPRESENT'] == '2', 999999).FSql.when(df_sinasc['TPAPRESENT'] == '3', 4218938).otherwise(999998).alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.TPAPRESENT.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: SEMAGESTAC
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, 1576063, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.semagestac)""") # SEMAGESTAC	Número de semanas de gestação.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
                                FSql.lit(1576063).cast(LongType()).alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.SEMAGESTAC.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: CODANOMAL
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, df_cid10.where(sqlLib.col('codigo_cid10').rlike('|'.join(replace(df_sinasc.codanomal,'.')))), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.codanomal)""") # CODANOMAL	Código da anomalia (CID 10). a consulta ao dataframe do cid10 deve retornar o concept_id correspondente ao cid10 de entrada.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc_cid10.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc_cid10.person_id.alias('person_id'), \
                                df_sinasc_cid10.concept_id.alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc_cid10.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc_cid10.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc_cid10.CODANOMAL.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: DTULTMENST
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, 4072438, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.dtultmenst)""") # DTULTMENST	Data da última menstruação (DUM): dd mm aaaa
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
                                FSql.lit(4072438).cast(LongType()).alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.DTULTMENST.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: GESTACAO
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.gestacao)""")  # GESTACAO	Semanas de gestação: 1– Menos de 22 semanas; 2– 22 a 27 semanas; 3– 28 a 31 semanas; 4– 32 a 36 semanas; 5– 37 a 41 semanas; 6– 42 semanas e mais; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['GESTACAO'] == '1', 999999).\
                                    FSql.when(df_sinasc['GESTACAO'] == '2', 999999).\
                                    FSql.when(df_sinasc['GESTACAO'] == '3', 999999).\
                                    FSql.when(df_sinasc['GESTACAO'] == '4', 999999).\
                                    FSql.when(df_sinasc['GESTACAO'] == '5', 999999).\
                                    FSql.when(df_sinasc['GESTACAO'] == '6', 999999).\
                                    otherwise(999999).alias('condition_concept_id'), \
								FSql.FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.GESTACAO.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()


# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: GRAVIDEZ
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.gravidez = 1 then 4014295 when df_sinasc.gravidez = 2 then 4101844 when df_sinasc.gravidez = 3 then 4094046 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.gravidez)""") # GRAVIDEZ	Tipo de gravidez: 1– Única; 2– Dupla; 3– Tripla ou mais; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['GRAVIDEZ'] == '1', 999999).\
                                    FSql.when(df_sinasc['GRAVIDEZ'] == '2', 999999).\
                                    FSql.when(df_sinasc['GRAVIDEZ'] == '3', 999999).\
                                    otherwise(999999).alias('condition_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.GRAVIDEZ.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: CONSPRENAT
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, 4313474, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.consprenat)""")  # CONSPRENAT	Número de consultas pré‐natal
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(4313474).cast(LongType()).alias('condition_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.CONSPRENAT.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: KOTELCHUCK
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.kotelchuck)""") # KOTELCHUCK	1 Não fez pré-natal (Campo33=0); 2 Inadequado (Campo34>3 ou Campo34<=3 e Campo33<3); 3 Intermediário (Campo34<=3 e Campo33 entre 3 e 5); 4 Adequado (Campo34<=3 e Campo33=6); 5 Mais que adequado (Campo34<=3 e Campo33>=7); 6 Não Classificados (campos 33 ou 34, Nulo ou Ign)
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['KOTELCHUCK'] == '1', 999999).\
                                    FSql.when(df_sinasc['KOTELCHUCK'] == '2', 999999).\
                                    FSql.when(df_sinasc['KOTELCHUCK'] == '3', 999999).\
                                    FSql.when(df_sinasc['KOTELCHUCK'] == '4', 999999).\
                                    FSql.when(df_sinasc['KOTELCHUCK'] == '5', 999999).\
                                    otherwise(999999).alias('condition_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.KOTELCHUCK.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

# *************************************************************
#  CONDITION_OCCURRENCE - Persistência dos dados 
#  Source field: TPMETESTIM
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tpmetestim)""") # TPMETESTIM	Método utilizado. Valores: 1– Exame físico; 2– Outro método; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_cond_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['TPMETESTIM'] == '1', 999999).\
                                    FSql.when(df_sinasc['TPMETESTIM'] == '2', 999999).\
                                    otherwise(999999).alias('condition_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_start_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("condition_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('condition_type_concept_id'), \
								df_sinasc.TPMETESTIM.alias('condition_source_value')).rdd, \
								df_cond_occur_schema)

if df_cond_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.condition_occurrence")
	count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
	# persistindo os dados de observation_period no banco.
	df_cond_occur.writeTo("bios.condition_occurrence").append()

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

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
# *************************************************************
# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_proc_occur_schema = StructType([ \
StructField("procedure_occurrence_id", LongType(), False), \
StructField("person_id", LongType(), False), \
StructField("procedure_concept_id", LongType(), False), \
StructField("procedure_date", DateType(), False), \
StructField("procedure_end_date", DateType(), True), \
StructField("procedure_type_concept_id", LongType(), False), \
StructField("procedure_source_value", StringType(), True) \
])

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: PARTO
# *************************************************************
#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
#values (
#(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.parto = 1 then 999999 when df_sinasc.parto = 2 then 4015701 else 9999999), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.parto)""") # PARTO	Tipo de parto: 1– Vaginal; 2– Cesário; 9– Ignorado
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_proc_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['PARTO'] == '1', 999999).\
								FSql.when(df_sinasc['PARTO'] == '2', 999999).\
                                otherwise(999998).alias('procedure_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('procedure_type_concept_id'), \
								df_sinasc.PARTO.alias('procedure_source_value')).rdd, \
								df_proc_occur_schema)

if df_proc_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.procedure_occurrence")
	count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
	# persistindo os dados de observation_period no banco.
	df_proc_occur.writeTo("bios.procedure_occurrence").append()

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: STTRABPART
# *************************************************************
#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
#values (
#(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.sttrabpart = 1 then 4121586 when df_sinasc.sttrabpart = 2 then 9999999 when df_sinasc.sttrabpart = 3 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.sttrabpart)""") # STTRABPART	Trabalho de parto induzido? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_proc_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['STTRABPART'] == '1', 4121586).\
								FSql.when(df_sinasc['STTRABPART'] == '2', 999999).\
								FSql.when(df_sinasc['STTRABPART'] == '3', 999999).\
                                otherwise(999998).alias('procedure_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('procedure_type_concept_id'), \
								df_sinasc.STTRABPART.alias('procedure_source_value')).rdd, \
								df_proc_occur_schema)

if df_proc_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.procedure_occurrence")
	count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
	# persistindo os dados de observation_period no banco.
	df_proc_occur.writeTo("bios.procedure_occurrence").append()

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: CONSULTAS
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, montar mapeamento via vocabulário climaterna, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.consultas)""") # CONSULTAS	Número de consultas de pré‐natal. Valores: 1– Nenhuma; 2– de 1 a 3; 3– de 4 a 6; 4– 7 e mais; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_proc_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['CONSULTAS'] == '1', 999999).\
								FSql.when(df_sinasc['CONSULTAS'] == '2', 999999).\
								FSql.when(df_sinasc['CONSULTAS'] == '3', 999999).\
								FSql.when(df_sinasc['CONSULTAS'] == '4', 999999).\
                                otherwise(999998).alias('procedure_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('procedure_type_concept_id'), \
								df_sinasc.CONSULTAS.alias('procedure_source_value')).rdd, \
								df_proc_occur_schema)

if df_proc_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.procedure_occurrence")
	count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
	# persistindo os dados de observation_period no banco.
	df_proc_occur.writeTo("bios.procedure_occurrence").append()

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: MESPRENAT
# *************************************************************
#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_start_date,procedure_type_concept_id, procedure_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.mesprenat)""") # MESPRENAT	Mês de gestação em que iniciou o pré‐natal
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_proc_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(999999).cast(LongType()).alias('procedure_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('procedure_type_concept_id'), \
								df_sinasc.MESPRENAT.alias('procedure_source_value')).rdd, \
								df_proc_occur_schema)

if df_proc_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.procedure_occurrence")
	count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
	# persistindo os dados de observation_period no banco.
	df_proc_occur.writeTo("bios.procedure_occurrence").append()

# *************************************************************
#  PROCEDURE_OCCURRENCE - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: STCESPARTO
# *************************************************************
#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.stcesparto)""") # STCESPARTO	Cesárea ocorreu antes do trabalho de parto iniciar? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_proc_occur=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.when(df_sinasc['STCESPARTO'] == '1', 999999).\
								FSql.when(df_sinasc['STCESPARTO'] == '2', 999999).\
								FSql.when(df_sinasc['STCESPARTO'] == '3', 999999).\
                                otherwise(999998).alias('procedure_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_date"), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("procedure_end_date"), \
								FSql.lit(32848).cast(LongType()).alias('procedure_type_concept_id'), \
								df_sinasc.STCESPARTO.alias('procedure_source_value')).rdd, \
								df_proc_occur_schema)
if df_proc_occur.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.procedure_occurrence")
	count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
	# persistindo os dados de observation_period no banco.
	df_proc_occur.writeTo("bios.procedure_occurrence").append()


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

# *************************************************************
#  MEASUREMENT - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
# *************************************************************
# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_measurement_schema = StructType([ \
StructField("measurement_occurrence_id", LongType(), False), \
StructField("person_id", LongType(), False), \
StructField("measurement_concept_id", LongType(), False), \
StructField("measurement_date", DateType(), False), \
StructField("measurement_type_concept_id", LongType(), False), \
StructField("measurement_source_value", StringType(), True) \
])

# *************************************************************
#  MEASUREMENT - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: TPROBSON
# *************************************************************
#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
#values (
#(df_measurement.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.tprobson, df_sinasc.tprobson)""") # TPROBSON	Código do Grupo de Robson, gerado pelo sistema
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_measurement=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('measurement_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(9999999).cast(LongType()).alias('measurement_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("measurement_date"), \
								FSql.lit(32848).cast(LongType()).alias('measurement_type_concept_id'), \
								df_sinasc.TPROBSON.alias('measurement_source_value')).rdd, \
								df_measurement_schema)

if df_measurement.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.measurement")
	count_max_measurement = count_max_measurement_df.first().max_measurement
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
	# persistindo os dados de observation_period no banco.
	df_measurement.writeTo("bios.measurement").append()

# *************************************************************
#  MEASUREMENT - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: APGAR1
# *************************************************************
#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
#values (
#(df_measurement.identity, df_sinasc.identity, 4014304, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.apgar1, df_sinasc.apgar1)""") # APGAR1	Apgar no 1º minuto
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_measurement=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('measurement_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(4014304).cast(LongType()).alias('measurement_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("measurement_date"), \
								FSql.lit(32848).cast(LongType()).alias('measurement_type_concept_id'), \
								df_sinasc.APGAR1.alias('measurement_source_value')).rdd, \
								df_measurement_schema)

if df_measurement.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.measurement")
	count_max_measurement = count_max_measurement_df.first().max_measurement
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
	# persistindo os dados de observation_period no banco.
	df_measurement.writeTo("bios.measurement").append()

# *************************************************************
#  MEASUREMENT - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: APGAR5
# *************************************************************
#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
#values (
#(df_measurement.identity, df_sinasc.identity, 4016464, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.apgar5, df_sinasc.apgar5)""") # APGAR5	Apgar no 5º minuto
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_measurement=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('measurement_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(4016464).cast(LongType()).alias('measurement_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("measurement_date"), \
								FSql.lit(32848).cast(LongType()).alias('measurement_type_concept_id'), \
								df_sinasc.APGAR5.alias('measurement_source_value')).rdd, \
								df_measurement_schema)

if df_measurement.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.measurement")
	count_max_measurement = count_max_measurement_df.first().max_measurement
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
	# persistindo os dados de observation_period no banco.
	df_measurement.writeTo("bios.measurement").append()

# *************************************************************
#  MEASUREMENT - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: PESO
# *************************************************************
#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 32848value_as_number,measurement_source_value)
#values (
#(df_measurement.identity, df_sinasc.identity, 4264825, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.peso, df_sinasc.peso)""") # PESO	Peso ao nascer em gramas.
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_measurement=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('measurement_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(4264825).cast(LongType()).alias('measurement_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("measurement_date"), \
								FSql.lit(32848).cast(LongType()).alias('measurement_type_concept_id'), \
								df_sinasc.PESO.alias('measurement_source_value')).rdd, \
								df_measurement_schema)
if df_measurement.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.measurement")
	count_max_measurement = count_max_measurement_df.first().max_measurement
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
	# persistindo os dados de observation_period no banco.
	df_measurement.writeTo("bios.measurement").append()

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

# *************************************************************
#  OBSERVATION - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela observation, por isso, o dataframe é recriado trocando o campo de entrada.
# *************************************************************
# Definindo o novo esquema para suportar valores nulos e não-nulos.
df_observation_schema = StructType([ \
StructField("observation_id", LongType(), False), \
StructField("person_id", LongType(), False), \
StructField("observation_concept_id", LongType(), False), \
StructField("observation_date", DateType(), False), \
StructField("observation_type_concept_id", LongType(), False), \
StructField("observation_source_value", StringType(), True) \
])

# *************************************************************
#  OBSERVATION - Persistência dos dados 
#  A partir de um registro do source serão inseridos vários registros na tabela observation, por isso, o dataframe é recriado trocando o campo de entrada.
#  Source field: PARIDADE
# *************************************************************
#spark.sql("""insert into observation(observation_id,person_id,observation_concept_id,observation_start_date,observation_type_concept_id, observation_source_value)
#values
#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 32848, df_sinasc.paridade)""") # PARIDADE	
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_observation=spark.createDataFrame(df_sinasc.select( \
								FSql.lit(0).cast(LongType()).alias('observation_id'), \
								df_sinasc.person_id.alias('person_id'), \
								FSql.lit(9999999).cast(LongType()).alias('observation_concept_id'), \
								FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'ddMMyyyy').alias("observation_date"), \
								FSql.lit(32848).cast(LongType()).alias('observation_type_concept_id'), \
								df_sinasc.PARIDADE.alias('observation_source_value')).rdd, \
								df_observation_schema)

if df_observation.count() > 0:
	#obtem o max da tabela para usar na inserção de novos registros
	count_max_observation_df = spark.sql("SELECT greatest(max(observation_id),0) + 1 AS max_observation FROM bios.observation")
	count_max_observation = count_max_observation_df.first().max_observation
	#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
	# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
	# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
	df_observation = df_observation.withColumn("observation_id", monotonically_increasing_id())
	#sincroniza os id's gerados com o max(person_id) existente no banco de dados
	df_observation = df_observation.withColumn("observation_id", df_observation["observation_id"] + count_max_observation)
	# persistindo os dados de observation_period no banco.
	df_observation.writeTo("bios.observation").append()

#registro datasus_person (extension table to receive extras fields from SINASC/SIM)
#Create table datasus_person (
# person_id  bigint not null,
# system_source_id  integer not null, 
# mother_birth_date_source_value integer,
# mother_birth_date date,
# mother_years_of_study integer,
# mother_education_level integer,
# mother_education_level_aggregated integer,
# mother_marital_status  integer,
# mother_age   integer,
# mother_city_of_birth integer,
# mother_state_of_birth  integer,
# mother_race integer,
# mother_elementary_school integer,
# father_age   integer,
# responsible_document_type  integer,
# responsible_role_type integer,
# place_of_birth_type_source_value integer, 
# care_site_of_birth_source_value integer,
# mother_professional_occupation integer, 
# mother_country_of_origin integer, 
# number_of_dead_children integer, 
# number_of_living_children integer, 
# number_of_previous_pregnancies integer, 
# number_of_previous_cesareans integer, 
# number_of_previous_normal_born integer) 
# using iceberg;


# CODMUNNATU
# CODOCUPMAE
# CODUFNATU
# DTNASCMAE
# ESCMAE
# ESCMAE2010
# ESCMAEAGR1
# ESTCIVMAE
# IDADEMAE
# IDADEPAI
# NATURALMAE
# QTDFILMORT
# QTDFILVIVO
# QTDGESTANT
# QTDPARTCES
# QTDPARTNOR
# RACACORMAE
# SERIESCMAE
# TPDOCRESP
# TPFUNCRESP

# *************************************************************
#  DATASUS_PERSON - Persistência dos dados 
#  Para cada registro do source será criado um único correspondente na tabela DATASUS_PERSON
# *************************************************************
# Definindo o novo esquema para suportar valores nulos e não-nulos.

df_datasus_person_schema = StructType([ \
StructField("person_id", LongType(), False), \
StructField("system_source_id", IntegerType(), False), \
StructField("mother_birth_date_source_value", IntegerType(), True), \
StructField("mother_birth_date", DateType(), True), \
StructField("mother_years_of_study", IntegerType(), True), \
StructField("mother_education_level", IntegerType(), True), \
StructField("mother_education_level_aggregated", IntegerType(), True), \
StructField("mother_marital_status", IntegerType(), True), \
StructField("mother_age", IntegerType(), True), \
StructField("mother_city_of_birth", IntegerType(), True), \
StructField("mother_state_of_birth", IntegerType(), True), \
StructField("mother_race", IntegerType(), True), \
StructField("mother_elementary_school", IntegerType(), True), \
StructField("father_age", IntegerType(), True), \
StructField("responsible_document_type", IntegerType(), True), \
StructField("responsible_role_type", IntegerType(), True), \
StructField("place_of_birth_type_source_value", IntegerType(), True), \
StructField("care_site_of_birth_source_value", IntegerType(), True)\
])


# *************************************************************
#  DATASUS_PERSON - Persistência dos dados 
#  Para cada registro do source será criado um único correspondente na tabela DATASUS_PERSON
#  Source field: TPROBSON
# *************************************************************
# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
# e aplicando o novo esquema ao DataFrame e copiando os dados.
df_datasus_person=spark.createDataFrame(df_sinasc.select( \
								df_sinasc.person_id.alias('person_id'), \
                                FSql.lit(1).cast(IntegerType()).alias('system_source_id'), \
                                df_sinasc.DTNASCMAE.alias('mother_birth_date_source_value'), \
                                FSql.to_date(FSql.lpad(df_sinasc.DTNASCMAE,8,'0'), 'ddMMyyyy').alias('mother_birth_date'), \
                                df_sinasc.ESCMAE.alias('mother_years_of_study'), \
                                df_sinasc.ESCMAE2010.alias('mother_education_level'), \
                                df_sinasc.ESCMAEAGR1.alias('mother_education_level_aggregated'), \
                                df_sinasc.ESTCIVMAE.alias('mother_marital_status'), \
                                df_sinasc.IDADEMAE.alias('mother_age'), \
                                df_sinasc.CODMUNNATU.alias('mother_city_of_birth'), \
                                df_sinasc.CODUFNATU.alias('mother_state_of_birth'), \
                                df_sinasc.RACACORMAE.alias('mother_race'), \
                                df_sinasc.SERIESCMAE.alias('mother_elementary_school'), \
                                df_sinasc.IDADEPAI.alias('father_age'), \
                                df_sinasc.TPDOCRESP.alias('responsible_document_type'), \
                                df_sinasc.TPFUNCRESP.alias('responsible_role_type'), \
                                df_sinasc.LOCNASC.alias('place_of_birth_type_source_value'), \
                                df_sinasc.CODESTAB.alias('care_site_of_birth_source_value'), \
                                df_sinasc.CODOCUPMAE.alias('mother_professional_occupation'), \
                                df_sinasc.NATURALMAE.alias('mother_country_of_origin'), \
                                df_sinasc.QTDFILMORT.alias('number_of_dead_children'), \
                                df_sinasc.QTDFILVIVO.alias('number_of_living_children'), \
                                df_sinasc.QTDGESTANT.alias('number_of_previous_pregnancies'), \
                                df_sinasc.QTDPARTCES.alias('number_of_previous_cesareans'), \
                                df_sinasc.QTDPARTNOR.alias('number_of_previous_normal_born')).rdd, \
                                df_datasus_person_schema)

# persistindo os dados de observation_period no banco.
if df_datasus_person.count() > 0:
    df_datasus_person.writeTo("bios.datasus_person").append()



 
