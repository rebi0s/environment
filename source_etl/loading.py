###########################################################################################################################
# Purpose: Main routine to execute the different profiles related to load and manipulate data into CLIMATERNA Project database
#
# Command line syntax to execute with parameters
# spark-submit --py-files /caminho/arquivo.py --conf spark.driver.extraJavaOptions="-Dparam1=valor1 -Dparam2=valor2"
#
# Execution parameters to control the execution profile:
# arg[1]:profile
#      INIT: Remove all existing tables and create new ones without data.
#            arg[2]:filename => path and SQL filename to be executed
#      VOCAB_OMOP: Run a specific set of routines to load OMOP vocabulary from CSV files to OMOP database in Iceberg
#            arg[2]:filename => path to CSV files to be loaded as OMOP vocabulary
#      DATASUS: Run a specific set of routines to load foreign info like: care site, locations, IDC10, etc. One execution is required for each foreing info.
#            arg[2]:source system => indicates the origin of source file (i.e. CNES, CID10, CITY)
#            arg[3]:filename => path and source filename to be executed. Each source file can have different extensions
#      VOCAB_CTRNA: Run a specific set of routines to load CLIMATERNA vocabulary into OMOP database in Iceberg 
#      ETL: Run the ETL process from source files to target OMOP database in Iceberg
#            arg[2]:source system => indicates the origin of source file (i.e. SINASC, SIM, CLIMA)
#            arg[3]:filename => path and source filename to be loaded. The file must be in a parquet format
#
# The following environment variables are available to config the execution. To avoid null values, the variables receive
# default values, but those default values are not guarantee to work properly.
#
###########################################################################################################################

# inicialização
import os
import sys
import logging
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, FloatType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as FSql
from pyspark.sql import DataFrame
from utils import *
from utils_vocab_omop import *
from utils_datasus import *

logger = configLogger('main');
# o nível de log efetivamente registrado depende da variável de ambiente utilizada abaixo.
logHandler = addLogHandler(logger, os.getenv("CTRNA_LOG_LEVEL", "INFO"))

logger.info('Log configuration done')

spark = initSpark()

# Mensagens de log
logger.info('Spark session started.')

#get the number of input parameters
num_args = len(sys.argv)

if num_args == 1:
	logger.error("Check the command line usage. A profile with parameters has to be provided. The available choices are: INIT, VOCAB_OMOP, DATASUS, VOCAB_CTRNA, SINASC.")
	sys.exit(-1)

profiles = ['INIT','VOCAB_OMOP','DATASUS','VOCAB_CTRNA', 'SINASC', 'CLIMATE', 'SIM']

if sys.argv[1] not in profiles:
	logger.error("Check the command line usage. A profile not valid was used to invoke the script. The available choices are: INIT, VOCAB_OMOP, DATASUS, VOCAB_CTRNA, SINASC, CLIMATE and SIM.")
	sys.exit(-1)

if sys.argv[1] == 'INIT':
	try:
		logger.info("Starting the database initialization. All data will be removed and tables re-created.")
		if num_args != 3:
			logger.error("Check the command line usage. For INIT profile, is required 1 parameter. Usage: spark-submit loading.py INIT /path/filename.sql")
			sys.exit(-1)
		execute_sql_commands_from_file(spark, sys.argv[2], logger)
		logger.info(f"INIT Profile executed with filename: {sys.argv[2]}")
		sys.exit(0)
	except Exception as e:
		logger.error("Error while executing INIT profile on OMOP database: ", str(e))
		sys.exit(-1)

if sys.argv[1] == 'VOCAB_OMOP':
	try:
		if num_args != 3:
			logger.error("Check the command line usage. For VOCAB_OMOP profile, 1 parameter is required. Usage: spark-submit loading.py VOCAB_OMOP /path_to_csv_files")
			sys.exit(-1)
		logger.info("Initiating OMOP Vocabulary loading.")
		for filename in os.listdir(sys.argv[2]):
			if filename == "CONCEPT.csv":
				loadOMOPConcept(sys.argv[2], filename, spark, logger )
			if filename == "CONCEPT_CLASS.csv":
				loadOMOPConceptClass(sys.argv[2], filename, spark, logger )
			if filename == "CONCEPT_SYNONYM.csv":
				loadOMOPConceptSynonym(sys.argv[2], filename, spark, logger )
			if filename == "DRUG_STRENGTH.csv":
				loadOMOPDrugStrength(sys.argv[2], filename, spark, logger )
			if filename == "VOCABULARY.csv":
				loadOMOPVocabulary(sys.argv[2], filename, spark, logger )
			if filename == "CONCEPT_ANCESTOR.csv":
				loadOMOPConceptAncestor(sys.argv[2], filename, spark, logger )
			if filename == "CONCEPT_RELATIONSHIP.csv":
				loadOMOPConceptRelationship(sys.argv[2], filename, spark, logger )
			if filename == "DOMAIN.csv":
				loadOMOPDomain(sys.argv[2], filename, spark, logger )
			if filename == "RELATIONSHIP.csv":
				loadOMOPRelationship(sys.argv[2], filename, spark, logger )
		logger.info("OMOP Vocabulary succesfully loaded to OMOP database.")
		sys.exit(0)
	except Exception as e:
		logger.error("Error while writing data to OMOP Vocabulary: ", str(e))
		sys.exit(-1)

#*************************
# usage:
#  spark-submit loading.py DATASUS -city /path_to_folder_with_cities
#  spark-submit loading.py DATASUS -care /path_to_folder_with_care_sites
#  
#*************************

if sys.argv[1] == 'DATASUS':
	try:
		if num_args != 5:
			logger.error("Check the command line usage. For DATASUS the options are as below.")
			logger.error("Usage: ")
			logger.error("   spark-submit loading.py DATASUS -city /path_to_folder_with_cities file_name_with_cities")
			logger.error("   spark-submit loading.py DATASUS -care /path_to_folder_with_care_sites file_name_with_care_sites")
			sys.exit(-1)

		logger.info("Loading external data from DATASUS to OMOP database.")
		if sys.argv[2] == "-city":
		    #loadStates(spark, logger, sys.argv[3])
			loadLocationCityRebios(sys.argv[3], sys.argv[4], spark, logger)
		if sys.argv[2] == "-care":
			df_location_cnes = loadLocationCnesRebios(sys.argv[3], sys.argv[4], spark, logger)
			if df_location_cnes.count() > 0:
				#loadTypeOfUnit(spark, logger, sys.argv[3])
				loadCareSiteRebios(sys.argv[3], sys.argv[4], df_location_cnes, spark, logger)
				loadProviderRebios(spark, logger)
			if df_location_cnes.count() == 0:
				logger.info("External data from DATASUS was not loaded to OMOP database.")
				sys.exit(-1)
		if sys.argv[2] == "-idc10":
			loadIdc10(spark, logger, sys.argv[3])

		logger.info("External data from DATASUS succesfully loaded to OMOP database.")
		sys.exit(0)
	except Exception as e:
		logger.error(f"Error while loading DATASUS data to OMOP database: {str(e)}")
		sys.exit(-1)

if sys.argv[1] == 'VOCAB_CTRNA':
	try:
		logger.info("Loading CLIMATERNA Vocabulary.")
		logger.info("CLIMATERNA Vocabulary successfully loaded to OMOP Database.")
		sys.exit(0)
	except Exception as e:
		logger.error("Error while writing CLIMATERNA Vocabulary to OMOP database: ", str(e))
		sys.exit(-1)

if sys.argv[1] == 'SINASC':
	try:
		if num_args != 4:
			logger.error("Check the command line usage. For SINASC the options are as below.")
			logger.error("Usage: ")
			logger.error("   spark-submit loading.py SINASC /path_to_folder_with_source_file source_file_name")
			sys.exit(-1)

		logger.info("Initiating SINASC processing from source files to OMOP database.")
		####################################################################
		##  Leitura do arquivo de entrada (source)                        ##
		####################################################################

#|ORIGEM|CODCART|NUMREGCART|DTREGCART|CODESTAB|CODMUNNASC|LOCNASC|IDADEMAE|ESTCIVMAE|ESCMAE|CODOCUPMAE|QTDFILVIVO|QTDFILMORT|CODMUNRES|CODPAISRES|GESTACAO|GRAVIDEZ|PARTO|CONSULTAS|  DTNASC|HORANASC|SEXO|APGAR1|APGAR5|RACACOR|PESO|IDANOMAL|DTCADASTRO|CODANOMAL|NUMEROLOTE|VERSAOSIST|DTRECEBIM|DIFDATA|DTRECORIG|NATURALMAE|CODMUNNATU|SERIESCMAE|DTNASCMAE|RACACORMAE|QTDGESTANT|QTDPARTNOR|QTDPARTCES|IDADEPAI|DTULTMENST|SEMAGESTAC|TPMETESTIM|CONSPRENAT|MESPRENAT|TPAPRESENT|STTRABPART|STCESPARTO|TPROBSON|STDNEPIDEM|STDNNOVA|RACACOR_RN|RACACORN|ESCMAE2010|CODMUNCART|CODUFNATU|TPNASCASSI|ESCMAEAGR1|DTRECORIGA|TPFUNCRESP|TPDOCRESP|DTDECLARAC|PARIDADE|KOTELCHUCK|
#+------+-------+----------+---------+--------+----------+-------+--------+---------+------+----------+----------+----------+---------+----------+--------+--------+-----+---------+--------+--------+----+------+------+-------+----+--------+----------+---------+----------+----------+---------+-------+---------+----------+----------+----------+---------+----------+----------+----------+----------+--------+----------+----------+----------+----------+---------+----------+----------+----------+--------+----------+--------+----------+--------+----------+----------+---------+----------+----------+----------+----------+---------+----------+--------+----------+
#|     1|   5096|      null|     null| 2515598|    110002|      1|      24|        2|     4|      null|        01|      null|   120040|         1|       5|       1|    2|        4|07052010|    1155|   1|    09|    10|      1|3400|       2|  10062010|     null|  20100005|    2.2.08| 17062010|     41| 17062010|      null|      null|      null|     null|      null|      null|      null|      null|    null|      null|      null|      null|      null|     null|      null|      null|      null|    null|         0|       0|      null|    null|      null|      null|     null|      null|      null|      null|      null|     null|      null|    null|      null|
#|     1|   null|      null|     null| 3792595|    110011|      1|      22|        2|     4|    999992|        01|        00|   120040|         1|       5|       1|    2|        3|03062010|    0948|   1|    08|    09|      4|3950|       2|  01072010|     null|  20100007|    2.2.08| 06072010|     33| 06072010|      null|      null|      null|     null|      null|      null|      null|      null|    null|      null|      null|      null|      null|     null|      null|      null|      null|    null|         0|       0|      null|    null|      null|      null|     null|      null|      null|      null|      null|     null|      null|    null|      null|
#|     1|   null|      null|     null| 2798484|    110030|      1|      20|        1|     3|    999992|        01|        00|   120040|         1|       5|       1|    1|        4|23022010|    1123|   1|    08|    09|      1|3550|       2|  15032010|     null|  20100003|    2.2.07| 16042010|     52| 16042010|      null|      null|      null|     null|      null|      null|      null|      null|    null|      null|      null|      null|      null|     null|      null|      null|      null|    null|         0|       0|      null|    null|      null|      null|     null|      null|      null|      null|      null|     null|      null|    null|      null|

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
		# SEMAGESTAC	Semanas de gestação com dois algarismos. (Números com dois algarismos; 9- igonorado)
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


#>>> df.printSchema()
#root
# |-- APGAR1: string (nullable = true)
# |-- APGAR5: string (nullable = true)
# |-- CODANOMAL: string (nullable = true)
# |-- CODCART: string (nullable = true)
# |-- CODESTAB: string (nullable = true)
# |-- CODMUNCART: string (nullable = true)
# |-- CODMUNNASC: string (nullable = true)
# |-- CODMUNNATU: string (nullable = true)
# |-- CODMUNRES: string (nullable = true)
# |-- CODOCUPMAE: string (nullable = true)
# |-- CODPAISRES: string (nullable = true)
# |-- CODUFNATU: string (nullable = true)
# |-- CONSPRENAT: string (nullable = true)
# |-- CONSULTAS: string (nullable = true)
# |-- DIFDATA: string (nullable = true)
# |-- DTCADASTRO: string (nullable = true)
# |-- DTDECLARAC: string (nullable = true)
# |-- DTNASC: string (nullable = true)
# |-- DTNASCMAE: string (nullable = true)
# |-- DTRECEBIM: string (nullable = true)
# |-- DTRECORIG: string (nullable = true)
# |-- DTRECORIGA: string (nullable = true)
# |-- DTREGCART: string (nullable = true)
# |-- DTULTMENST: string (nullable = true)
# |-- ESCMAE: string (nullable = true)
# |-- ESCMAE2010: string (nullable = true)
# |-- ESCMAEAGR1: string (nullable = true)
# |-- ESTCIVMAE: string (nullable = true)
# |-- GESTACAO: string (nullable = true)
# |-- GRAVIDEZ: string (nullable = true)
# |-- HORANASC: string (nullable = true)
# |-- IDADEMAE: string (nullable = true)
# |-- IDADEPAI: string (nullable = true)
# |-- IDANOMAL: string (nullable = true)
# |-- KOTELCHUCK: string (nullable = true)
# |-- LOCNASC: string (nullable = true)
# |-- MESPRENAT: string (nullable = true)
# |-- NATURALMAE: string (nullable = true)
# |-- NUMEROLOTE: string (nullable = true)
# |-- NUMREGCART: string (nullable = true)
# |-- ORIGEM: string (nullable = true)
# |-- PARIDADE: string (nullable = true)
# |-- PARTO: string (nullable = true)
# |-- PESO: string (nullable = true)
# |-- QTDFILMORT: string (nullable = true)
# |-- QTDFILVIVO: string (nullable = true)
# |-- QTDGESTANT: string (nullable = true)
# |-- QTDPARTCES: string (nullable = true)
# |-- QTDPARTNOR: string (nullable = true)
# |-- RACACOR_RN: string (nullable = true)
# |-- RACACOR: string (nullable = true)
# |-- RACACORMAE: string (nullable = true)
# |-- RACACORN: string (nullable = true)
# |-- SEMAGESTAC: string (nullable = true)
# |-- SERIESCMAE: string (nullable = true)
# |-- SEXO: string (nullable = true)
# |-- STCESPARTO: string (nullable = true)
# |-- STDNEPIDEM: string (nullable = true)
# |-- STDNNOVA: string (nullable = true)
# |-- STTRABPART: string (nullable = true)
# |-- TPAPRESENT: string (nullable = true)
# |-- TPDOCRESP: string (nullable = true)
# |-- TPFUNCRESP: string (nullable = true)
# |-- TPMETESTIM: string (nullable = true)
# |-- TPNASCASSI: string (nullable = true)
# |-- TPROBSON: string (nullable = true)
# |-- VERSAOSIST: string (nullable = true)


		#carga dos dados do parquet do SINASC
		#source_path = os.getenv("CTRNA_SOURCE_SINASC_PATH","/home/etl-rebios/")
		#arquivo_entrada = "sinasc_2010_2022.parquet"

		# leitura do sinasc original em formato parquet
		if not os.path.isfile(os.path.join(sys.argv[2], sys.argv[3])):
				logger.info("Arquivo SINASC não localizado. Carga interrompida.")
				sys.exit(-1)

		logger.info("Loading file: %s ", os.path.join(sys.argv[2], sys.argv[3]))
		df_sinasc = spark.read.parquet(os.path.join(sys.argv[2], sys.argv[3]))
		df_sinasc.count()
		source_filename = os.path.join(sys.argv[2], sys.argv[3])
		ingestion_timestamp = datetime.now()

		# column HORANASC is presenting null values on source file and that will be filled with 0000
		df_sinasc=df_sinasc.fillna({"HORANASC": "0000"})

		# column DATANASC is presenting null values on source file and those records will be discarded
		df_sinasc=df_sinasc.filter((df_sinasc["DTNASC"].isNull() == False) & (df_sinasc["DTNASC"].rlike("^[0-9]{8}$")))
		#df_sinasc=df_sinasc.filter((df_sinasc["HORANASC"].isNull() == False))
		#df_sinasc=df_sinasc.filter((df_sinasc["HORANASC"].rlike("^[0-9]{4}$")) & (~df_sinasc["HORANASC"].rlike("[^0-9]")))
		#df_sinasc=df_sinasc.filter((df_sinasc["HORANASC"] <= 2359))
		df_sinasc = df_sinasc.withColumn("HORANASC", FSql.when(((df_sinasc["HORANASC"] > 2359) | ~(df_sinasc["HORANASC"].rlike("^[0-9]{4}$")) | (~df_sinasc["HORANASC"].rlike("[^0-9]"))) == True, "0000").otherwise(df_sinasc["HORANASC"]))


        # |-- LOCNASC <- as.factor(.data$LOCNASC): string (nullable = true)
        # |-- IDADEMAE <- as.factor(.data$IDADEMAE): string (nullable = true)
        # |-- ESTCIVMAE <- as.factor(.data$ESTCIVMAE): string (nullable = true)
        # |-- ESCMAE <- as.factor(.data$ESCMAE): string (nullable = true)
        # |-- CODOCUPMAE: string (nullable = true)
        # |-- GESTACAO <- as.factor(.data$GESTACAO): string (nullable = true)
        # |-- PARTO <- as.factor(.data$PARTO): string (nullable = true)


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

        # dataframe with records of Type Of Health Unit
		#df_cnes_tpunid = spark.read.format("iceberg").load(f"bios.rebios.type_of_unit")
		# dataframe with existing location records representing only cities
		logger.info("Loading info from Locations...")
		df_location = spark.read.format("iceberg").load(f"bios.rebios.location").filter(FSql.col("county").isNotNull())
		# dataframe with existing care_sites
		logger.info("Loading info from Care Sites...")
		df_care_site = spark.read.format("iceberg").load(f"bios.rebios.care_site")
		# dataframe with existing providers
		logger.info("Loading info from Providers...")
		df_provider = spark.read.format("iceberg").load(f"bios.rebios.provider")

		# dataframe with concept records
		#df_concept = spark.read.format("iceberg").load(f"bios.rebios.concept")

		#obtem o max person_id para usar na inserção de novos registros
		count_max_person_df = spark.sql("SELECT greatest(max(person_id),0) + 1 AS max_person FROM bios.rebios.person")
		count_max_person = count_max_person_df.first().max_person
		#geração dos id's únicos nos dados de entrada. O valor inicial é 0.
		# a função monotonically_increasing_id() gera números incrementais com a garantia de ser sempre maior que os existentes.
		df_sinasc = df_sinasc.withColumn("person_id", monotonically_increasing_id())
		#sincroniza os id's gerados com o max(person_id) existente no banco de dados atualizando os registros no df antes de escrever no banco
		df_sinasc = df_sinasc.withColumn("person_id", df_sinasc["person_id"] + count_max_person)

		# esses df's poderão conter valores nulos para município e estabelecimento de saúde, caso não haja cadastro.
		# a partir da coluna person_id, os registros de entrada se tornam unicamente identificados.
		# left outer join entre sinasc e location para associar dados de município
		#df_sinasc_location = (df_sinasc.join(df_location, on=['df_sinasc.CODMUNRES == df_location.location_id'], how='left'))
		df_sinasc = (df_sinasc.join(df_location, [df_sinasc.CODMUNRES == df_location.location_source_value.substr(1, 6)], 'left'))
		df_sinasc = df_sinasc.withColumnRenamed("location_id","location_id_city")
		# left outer join entre sinasc e care site para associar dados de estabelecimento de saúde
		df_sinasc = (df_sinasc.join(df_care_site, [df_sinasc.CODESTAB == df_care_site.care_site_source_value], 'left'))
		df_sinasc = df_sinasc.withColumnRenamed("location_id","location_id_care_site")

		#####  CRIAR O DF PARA CONTENDO O PERSON_ID E O CID10 CORRESPONDENTE
		# left outer join entre sinasc e vocabulário para associar dados de 
		#df_sinasc_cid10 = (df_sinasc.join(df_concept, on=['df_sinasc.codmunres == df_concept.location_id'], how='left'))

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
			StructField("birth_timestamp", TimestampType(), True), \
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

		logger.info("Processing Person data...")
		df_person = spark.createDataFrame(df_sinasc.select(
			FSql.col("person_id"),
			FSql.when((FSql.col("SEXO") == 'M') | (FSql.col("SEXO") == '1') | (FSql.col("SEXO") == 'Masculino'), 8507)
			.when((FSql.col("SEXO") == 'F') | (FSql.col("SEXO") == '2') | (FSql.col("SEXO") == 'Feminino'), 8532)
			.otherwise(8551).alias("gender_concept_id"),
			FSql.coalesce(FSql.year(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')), FSql.lit('0000').cast(IntegerType())).alias("year_of_birth"),
			FSql.coalesce(FSql.month(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')), FSql.lit('00').cast(IntegerType())).alias("month_of_birth"),
			FSql.coalesce(FSql.dayofmonth(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')), FSql.lit('00').cast(IntegerType())).alias("day_of_birth"),
			FSql.to_timestamp(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')).alias("birth_timestamp"),
			FSql.when ((FSql.col("RACACOR") == 1) | (FSql.col("RACACOR") == 'Branca'), 3212942)
			.when((FSql.col("RACACOR") == 2) | (FSql.col("RACACOR") == 'Preta'), 3213733)
			.when((FSql.col("RACACOR") == 3) | (FSql.col("RACACOR") == 'Amarela'), 3213498)
			.when((FSql.col("RACACOR") == 4) | (FSql.col("RACACOR") == 'Parda'), 3213487)
			.when((FSql.col("RACACOR") == 5) | (FSql.col("RACACOR") == 'Indígena'), 3213694)
			.otherwise(763013).alias("race_concept_id"),
			FSql.lit(38003563).alias('ethnicity_concept_id'),
			FSql.col("location_id_city").alias('location_id'),
			FSql.lit(None).cast(StringType()).alias('provider_id'),
			FSql.col("care_site_id").alias('care_site_id'),
			FSql.lit(None).cast(StringType()).alias('person_source_value'),
			FSql.col("SEXO").alias('gender_source_value'),
			FSql.lit(None).cast(StringType()).alias('gender_source_concept_id'),
			FSql.coalesce(FSql.col("RACACOR"), FSql.lit('Nulo')).alias('race_source_value'),
			FSql.lit(None).cast(StringType()).alias('race_source_concept_id'),
			FSql.lit(None).cast(StringType()).alias('ethnicity_source_value'),
			FSql.lit(None).cast(StringType()).alias('ethnicity_source_concept_id')
		).rdd, df_person_schema)


		if df_person.count() > 0:
			# Persistindo os dados de person no banco.
			df_person.show()
			df_person.writeTo("bios.rebios.person").append()
			logger.info("Table Person was succesfully updated with SINASC data.")
		else:
			logger.error("Error on processing Person data.")
			exit(-1)

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
			StructField("observation_period_start_date", TimestampType(), False), \
			StructField("observation_period_end_date", TimestampType(), False), \
			StructField("period_type_concept_id", LongType(), False) \
		])

		#FSql.to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias("observation_period_start_date"), \
		#FSql.to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias('observation_period_end_date'), \

		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		logger.info("Processing Observation Period data...")
		df_obs_period=spark.createDataFrame(df_sinasc.select(\
		FSql.lit(0).cast(LongType()).alias('observation_period_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), FSql.lit('01011991 0000')), 'ddMMyyyy HHmm')).alias("observation_period_start_date"),\
		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORANASC"), FSql.lit('0000')), 4, '0')), FSql.lit('01011991 0000')), 'ddMMyyyy HHmm')).alias("observation_period_end_date"),\
		FSql.lit(4193440).alias('period_type_concept_id')).rdd, \
		df_obs_period_schema)

		#df_null = df_obs_period.filter(df_obs_period["observation_period_start_date"].isNull() == True)
		#df_null.show(n=999999, truncate=False)

		if df_obs_period.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_obs_period_df = spark.sql("SELECT greatest(max(observation_period_id),0) + 1 AS max_obs_period FROM bios.rebios.observation_period")
			count_max_obs_period = count_max_obs_period_df.first().max_obs_period
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_obs_period = df_obs_period.withColumn("observation_period_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_obs_period = df_obs_period.withColumn("observation_period_id", df_obs_period["observation_period_id"] + count_max_obs_period)
			# persistindo os dados de observation_period no banco.
			df_obs_period.writeTo("bios.rebios.observation_period").append()
			logger.info("Table Obervation Period was succesfully updated with SINASC data.")




		####################################################################
		##  Persistir os dados no Climaterna com as consistências feitas  ##
		####################################################################

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela condition_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: STDNEPIDEM
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.stdnepidem = 1 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.stdnepidem)""") # STDNEPIDEM	Status de DN Epidemiológica. Valores: 1 – SIM; 0 – NÃO.

		# Definindo o novo esquema para suportar valores nulos e não-nulos.
		df_cond_occur_schema = StructType([ \
		StructField("condition_occurrence_id", LongType(), False), \
		StructField("person_id", LongType(), False), \
		StructField("condition_concept_id", LongType(), False), \
		StructField("condition_start_date", DateType(), False), \
        StructField("condition_start_timestamp", TimestampType(), True), \
		StructField("condition_end_date", DateType(), True), \
        StructField("condition_end_timestamp", TimestampType(), True), \
		StructField("condition_type_concept_id", LongType(), False), \
		StructField("condition_status_concept_id", LongType(), True), \
		StructField("stop_reason", StringType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id", LongType(), True), \
		StructField("visit_detail_id", LongType(), True), \
		StructField("condition_source_value", StringType(), True), \
		StructField("condition_source_concept_id", LongType(), True), \
		StructField("condition_status_source_value", StringType(), True)
		])

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(432250).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.STDNEPIDEM.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [STDNEPIDEM] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: TPAPRESENT
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.tpapresent = 1 then 9999999 when df_sinasc.tpapresent = 2 then 9999999 when df_sinasc.tpapresent = 3 then 4218938 else 9999999 , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.tpapresent)""") # TPAPRESENT	Tipo de apresentação do RN. Valores: 1– Cefálico; 2– Pélvica ou podálica; 3– Transversa; 9– Ignorado.

		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(40338816).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.TPAPRESENT.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [TPAPRESENT] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: SEMAGESTAC
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, 1576063, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.semagestac)""") # SEMAGESTAC	Número de semanas de gestação.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(40608797).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.SEMAGESTAC.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)


		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [SEMAGESTAC] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: CODANOMAL
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, df_cid10.where(sqlLib.col('codigo_cid10').rlike('|'.join(replace(df_sinasc.codanomal,'.')))), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.codanomal)""") # CODANOMAL	Código da anomalia (CID 10). a consulta ao dataframe do cid10 deve retornar o concept_id correspondente ao cid10 de entrada.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4079975).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.CODANOMAL.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [CODANOMAL] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: DTULTMENST
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, 4072438, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.dtultmenst)""") # DTULTMENST	Data da última menstruação (DUM): dd mm aaaa
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4072438).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.DTULTMENST.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)


		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [DTULTMENST] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: GESTACAO
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.gestacao)""")  # GESTACAO	Semanas de gestação: 1– Menos de 22 semanas; 2– 22 a 27 semanas; 3– 28 a 31 semanas; 4– 32 a 36 semanas; 5– 37 a 41 semanas; 6– 42 semanas e mais; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(44789950).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.GESTACAO.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [GESTACAO] was succesfully updated with SINASC data.")


		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: GRAVIDEZ
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.gravidez = 1 then 4014295 when df_sinasc.gravidez = 2 then 4101844 when df_sinasc.gravidez = 3 then 4094046 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.gravidez)""") # GRAVIDEZ	Tipo de gravidez: 1– Única; 2– Dupla; 3– Tripla ou mais; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(1030947).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.GRAVIDEZ.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)


		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [GRAVIDEZ] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: CONSPRENAT
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, 4313474, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.consprenat)""")  # CONSPRENAT	Número de consultas pré‐natal
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "CONSPRENAT" for field in df_sinasc.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sinasc.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sinasc.person_id.alias('person_id'), \
			FSql.lit(4313474).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sinasc.CONSPRENAT.alias('condition_source_value'), \
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [CONSPRENAT] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: KOTELCHUCK
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.kotelchuck)""") # KOTELCHUCK	1 Não fez pré-natal (Campo33=0); 2 Inadequado (Campo34>3 ou Campo34<=3 e Campo33<3); 3 Intermediário (Campo34<=3 e Campo33 entre 3 e 5); 4 Adequado (Campo34<=3 e Campo33=6); 5 Mais que adequado (Campo34<=3 e Campo33>=7); 6 Não Classificados (campos 33 ou 34, Nulo ou Ign)
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.

		if any(field.name == "KOTELCHUCK" for field in df_sinasc.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sinasc.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sinasc.person_id.alias('person_id'), \
			FSql.lit(4112701).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sinasc.KOTELCHUCK.alias('condition_source_value'), \
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [KOTELCHUCK] was succesfully updated with SINASC data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  Source field: TPMETESTIM
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.tpmetestim)""") # TPMETESTIM	Método utilizado. Valores: 1– Exame físico; 2– Outro método; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(42538440).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.TPMETESTIM.alias('condition_source_value'), \
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)


		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [TPMETESTIM] was succesfully updated with SINASC data.")

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
		StructField("procedure_timestamp", TimestampType(), True), \
		StructField("procedure_end_date", DateType(), True), \
		StructField("procedure_end_timestamp", TimestampType(), True), \
		StructField("procedure_type_concept_id", LongType(), False), \
		StructField("modifier_concept_id", LongType(), True), \
		StructField("quantity", IntegerType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id",  LongType(), True), \
		StructField("visit_detail_id",  LongType(), True), \
		StructField("procedure_source_value", StringType(), True), \
		StructField("procedure_source_concept_id", LongType(), True), \
		StructField("modifier_source_value",  StringType(), True) \
		])

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: PARTO
		# *************************************************************
		#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
		#values (
		#(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.parto = 1 then 999999 when df_sinasc.parto = 2 then 4015701 else 9999999), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.parto)""") # PARTO	Tipo de parto: 1– Vaginal; 2– Cesário; 9– Ignorado
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4264420).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.PARTO.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [PARTO] was succesfully updated with SINASC data.")

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: STTRABPART
		# *************************************************************
		#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
		#values (
		#(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.sttrabpart = 1 then 4121586 when df_sinasc.sttrabpart = 2 then 9999999 when df_sinasc.sttrabpart = 3 then 999999 else 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.sttrabpart)""") # STTRABPART	Trabalho de parto induzido? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4014719).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.STTRABPART.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [STTRABPART] was succesfully updated with SINASC data.")

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: CONSULTAS
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, montar mapeamento via vocabulário climaterna, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.consultas)""") # CONSULTAS	Número de consultas de pré‐natal. Valores: 1– Nenhuma; 2– de 1 a 3; 3– de 4 a 6; 4– 7 e mais; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4313474).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.CONSULTAS.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [CONSULTAS] was succesfully updated with SINASC data.")

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: MESPRENAT
		# *************************************************************
		#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_start_date,procedure_type_concept_id, procedure_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.mesprenat)""") # MESPRENAT	Mês de gestação em que iniciou o pré‐natal
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4311447).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.MESPRENAT.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [MESPRENAT] was succesfully updated with SINASC data.")

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: STCESPARTO
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.stcesparto)""") # STCESPARTO	Cesárea ocorreu antes do trabalho de parto iniciar? Valores: 1– Sim; 2– Não; 3– Não se aplica; 9– Ignorado.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4053609).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.STCESPARTO.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)


		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [STCESPARTO] was succesfully updated with SINASC data.")


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
		StructField("measurement_id", LongType(), False), \
		StructField("person_id", LongType(), False), \
		StructField("measurement_concept_id", LongType(), False), \
		StructField("measurement_date", DateType(), False), \
		StructField("measurement_type_concept_id", LongType(), False), \
		StructField("value_as_number", FloatType(), True), \
		StructField("measurement_timestamp", TimestampType(), True), \
		StructField("measurement_time", TimestampType(), True), \
		StructField("operator_concept_id", LongType(), True), \
		StructField("value_as_concept_id", LongType(), True), \
		StructField("unit_concept_id", LongType(), True), \
		StructField("range_low", FloatType(), True), \
		StructField("range_high", FloatType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id", LongType(), True), \
		StructField("visit_detail_id", LongType(), True), \
		StructField("measurement_source_value", StringType(), True), \
		StructField("measurement_source_concept_id", LongType(), True), \
		StructField("unit_source_value", StringType(), True), \
		StructField("unit_source_concept_id", LongType(), True), \
		StructField("value_source_value", StringType(), True), \
		StructField("measurement_event_id", LongType(), True), \
		StructField("meas_event_field_concept_id", LongType(), True) \
		])

		# *************************************************************
		#  MEASUREMENT - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: TPROBSON
		# *************************************************************
		#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 4216316value_as_number,measurement_source_value)
		#values (
		#(df_measurement.identity, df_sinasc.identity, 9999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.tprobson, df_sinasc.tprobson)""") # TPROBSON	Código do Grupo de Robson, gerado pelo sistema
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "TPROBSON" for field in df_sinasc.schema.fields):
			df_measurement=spark.createDataFrame(df_sinasc.select( \
			FSql.lit(0).cast(LongType()).alias('measurement_id'), \
			df_sinasc.person_id.alias('person_id'), \
			FSql.lit(4019758).cast(LongType()).alias('measurement_concept_id'), \
			FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('measurement_date'), \
			FSql.lit(4216316).cast(LongType()).alias('measurement_type_concept_id'), \
			FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
			FSql.lit(None).cast(TimestampType()).alias('measurement_timestamp'), \
			FSql.lit(None).cast(TimestampType()).alias('measurement_time'), \
			FSql.lit(None).cast(LongType()).alias('operator_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
			FSql.lit(None).cast(FloatType()).alias('range_low'), \
			FSql.lit(None).cast(FloatType()).alias('range_high'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sinasc.TPROBSON.alias('measurement_source_value'), \
			FSql.lit(None).cast(LongType()).alias('measurement_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
			FSql.lit(None).cast(LongType()).alias('unit_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('value_source_value'), \
			FSql.lit(None).cast(LongType()).alias('measurement_event_id'), \
			FSql.lit(None).cast(LongType()).alias('meas_event_field_concept_id') \
			).rdd, df_measurement_schema)

			if df_measurement.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.rebios.measurement")
				count_max_measurement = count_max_measurement_df.first().max_measurement
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
				# persistindo os dados de observation_period no banco.
				df_measurement.writeTo("bios.rebios.measurement").append()
				logger.info("Table Measurement Occurrence [TPROBSON] was succesfully updated with SINASC data.")

		# *************************************************************
		#  MEASUREMENT - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: APGAR1
		# *************************************************************
		#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 4216316value_as_number,measurement_source_value)
		#values (
		#(df_measurement.identity, df_sinasc.identity, 4014304, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.apgar1, df_sinasc.apgar1)""") # APGAR1	Apgar no 1º minuto
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_measurement=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('measurement_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4014304).cast(LongType()).alias('measurement_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('measurement_date'), \
		FSql.lit(4216316).cast(LongType()).alias('measurement_type_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_timestamp'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_time'), \
		FSql.lit(None).cast(LongType()).alias('operator_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('range_low'), \
		FSql.lit(None).cast(FloatType()).alias('range_high'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.APGAR1.alias('measurement_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
		FSql.lit(None).cast(LongType()).alias('unit_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('value_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_event_id'), \
		FSql.lit(None).cast(LongType()).alias('meas_event_field_concept_id') \
		).rdd, df_measurement_schema)

		if df_measurement.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.rebios.measurement")
			count_max_measurement = count_max_measurement_df.first().max_measurement
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
			# persistindo os dados de observation_period no banco.
			df_measurement.writeTo("bios.rebios.measurement").append()
			logger.info("Table Measurement Occurrence [APGAR1] was succesfully updated with SINASC data.")

		# *************************************************************
		#  MEASUREMENT - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: APGAR5
		# *************************************************************
		#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 4216316value_as_number,measurement_source_value)
		#values (
		#(df_measurement.identity, df_sinasc.identity, 4016464, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.apgar5, df_sinasc.apgar5)""") # APGAR5	Apgar no 5º minuto
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_measurement=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('measurement_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4016464).cast(LongType()).alias('measurement_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('measurement_date'), \
		FSql.lit(4216316).cast(LongType()).alias('measurement_type_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_timestamp'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_time'), \
		FSql.lit(None).cast(LongType()).alias('operator_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('range_low'), \
		FSql.lit(None).cast(FloatType()).alias('range_high'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.APGAR5.alias('measurement_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
		FSql.lit(None).cast(LongType()).alias('unit_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('value_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_event_id'), \
		FSql.lit(None).cast(LongType()).alias('meas_event_field_concept_id') \
		).rdd, df_measurement_schema)


		if df_measurement.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.rebios.measurement")
			count_max_measurement = count_max_measurement_df.first().max_measurement
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
			# persistindo os dados de observation_period no banco.
			df_measurement.writeTo("bios.rebios.measurement").append()
			logger.info("Table Measurement Occurrence [APGAR5] was succesfully updated with SINASC data.")

		# *************************************************************
		#  MEASUREMENT - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: PESO
		# *************************************************************
		#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 4216316value_as_number,measurement_source_value)
		#values (
		#(df_measurement.identity, df_sinasc.identity, 4264825, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.peso, df_sinasc.peso)""") # PESO	Peso ao nascer em gramas.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_measurement=spark.createDataFrame(df_sinasc.select( \
		FSql.lit(0).cast(LongType()).alias('measurement_id'), \
		df_sinasc.person_id.alias('person_id'), \
		FSql.lit(4264825).cast(LongType()).alias('measurement_concept_id'), \
		FSql.to_date(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('measurement_date'), \
		FSql.lit(4216316).cast(LongType()).alias('measurement_type_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_timestamp'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_time'), \
		FSql.lit(None).cast(LongType()).alias('operator_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('range_low'), \
		FSql.lit(None).cast(FloatType()).alias('range_high'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sinasc.PESO.alias('measurement_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
		FSql.lit(None).cast(LongType()).alias('unit_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('value_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_event_id'), \
		FSql.lit(None).cast(LongType()).alias('meas_event_field_concept_id') \
		).rdd, df_measurement_schema)

		if df_measurement.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.rebios.measurement")
			count_max_measurement = count_max_measurement_df.first().max_measurement
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
			# persistindo os dados de observation_period no banco.
			df_measurement.writeTo("bios.rebios.measurement").append()
			logger.info("Table Measurement Occurrence [PESO] was succesfully updated with SINASC data.")

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
		# usado type_concept  Government Report 4216316

		# *************************************************************
		#  OBSERVATION - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela observation, por isso, o dataframe é recriado trocando o campo de entrada.
		# *************************************************************
		# Definindo o novo esquema para suportar valores nulos e não-nulos.
		df_observation_schema = StructType([ \
		StructField("observation_id", LongType(), False), \
		StructField("person_id", LongType(), False), \
		StructField("observation_concept_id", LongType(), False), \
		StructField("observation_date", TimestampType(), False), \
		StructField("observation_timestamp", TimestampType(), True), \
		StructField("observation_type_concept_id", LongType(), False), \
		StructField("value_as_number", FloatType(), True), \
		StructField("value_source_value", StringType(), True), \
		StructField("value_as_string", StringType(), True), \
		StructField("value_as_concept_id", LongType(), True), \
		StructField("qualifier_concept_id", LongType(), True), \
		StructField("unit_concept_id", LongType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id", LongType(), True), \
		StructField("visit_detail_id", LongType(), True), \
		StructField("observation_source_value", StringType(), True), \
		StructField("observation_source_concept_id", LongType(), True), \
		StructField("unit_source_value", StringType(), True), \
		StructField("qualifier_source_value", StringType(), True), \
		StructField("observation_event_id", LongType(), True), \
		StructField("obs_event_field_concept_id", LongType(), True)	\
		])

		# *************************************************************
		#  OBSERVATION - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela observation, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: PARIDADE
		# *************************************************************
		#spark.sql("""insert into observation(observation_id,person_id,observation_concept_id,observation_start_date,observation_type_concept_id, observation_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, , makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.paridade)""") # PARIDADE	
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "PARIDADE" for field in df_sinasc.schema.fields):
			df_observation=spark.createDataFrame(df_sinasc.select( \
			FSql.lit(0).cast(LongType()).alias('observation_id'), \
			df_sinasc.person_id.alias('person_id'), \
			FSql.lit(4060186).cast(LongType()).alias('observation_concept_id'), \
			FSql.to_timestamp(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('observation_date'), \
			FSql.lit(None).cast(TimestampType()).alias('observation_timestamp'), \
			FSql.lit(4216316).cast(LongType()).alias('observation_type_concept_id'), \
			FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
			FSql.lit(None).cast(StringType()).alias('value_source_value'), \
			FSql.lit(None).cast(StringType()).alias('value_as_string'), \
			FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('qualifier_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sinasc.PARIDADE.alias('observation_source_value'), \
			FSql.lit(None).cast(LongType()).alias('observation_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
			FSql.lit(None).cast(StringType()).alias('qualifier_source_value'), \
			FSql.lit(None).cast(LongType()).alias('observation_event_id'), \
			FSql.lit(None).cast(LongType()).alias('obs_event_field_concept_id'), \
			).rdd, df_observation_schema)

			if df_observation.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_observation_df = spark.sql("SELECT greatest(max(observation_id),0) + 1 AS max_observation FROM bios.rebios.observation")
				count_max_observation = count_max_observation_df.first().max_observation
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_observation = df_observation.withColumn("observation_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_observation = df_observation.withColumn("observation_id", df_observation["observation_id"] + count_max_observation)
				# persistindo os dados de observation_period no banco.
				df_observation.writeTo("bios.rebios.observation").append()
				logger.info("Table Observation [PARIDADE] was succesfully updated with SINASC data.")

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
		StructField("datasus_person_source_date", DateType(), False), \
		StructField("ingestion_timestamp", TimestampType(), False), \
		StructField("source_file", StringType(), False), \
		StructField("mother_birth_date_source_value", StringType(), True), \
		StructField("mother_birth_date", DateType(), True), \
		StructField("mother_years_of_study", StringType(), True), \
		StructField("mother_marital_status", StringType(), True), \
		StructField("mother_age", IntegerType(), True), \
		StructField("mother_city_of_birth", IntegerType(), True), \
		StructField("mother_race", StringType(), True), \
		StructField("mother_elementary_school", IntegerType(), True), \
		StructField("father_age", IntegerType(), True), \
		StructField("place_of_birth_type_source_value", StringType(), True), \
		StructField("care_site_of_birth_source_value", IntegerType(), True), \
		StructField("mother_professional_occupation", StringType(), True), \
		StructField("mother_country_of_origin", IntegerType(), True), \
		StructField("number_of_dead_children", IntegerType(), True), \
		StructField("number_of_living_children", IntegerType(), True), \
		StructField("number_of_previous_pregnancies", IntegerType(), True), \
		StructField("number_of_previous_cesareans", IntegerType(), True), \
		StructField("number_of_previous_normal_born", IntegerType(), True), \
		StructField("mother_education_level", StringType(), True), \
		StructField("mother_education_level_aggregated", StringType(), True), \
		StructField("mother_state_of_birth", IntegerType(), True), \
		StructField("responsible_document_type", IntegerType(), True), \
		StructField("responsible_role_type", IntegerType(), True)\
		])

		# *************************************************************
		#  DATASUS_PERSON - Persistência dos dados 
		#  Para cada registro do source será criado um único correspondente na tabela DATASUS_PERSON
		#  Source field: TPROBSON
		# *************************************************************
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
#		datasus_person=spark.createDataFrame(df_sinasc.select( \
#		df_sinasc.person_id.alias('person_id'), \
#		FSql.lit(1).cast(IntegerType()).alias('system_source_id'), \
#		df_sinasc.DTNASCMAE.alias('mother_birth_date_source_value'), \
#		FSql.to_date(FSql.lpad(df_sinasc.DTNASCMAE,10,'0'), 'DDmmyyyy').alias('mother_birth_date'), \
#		df_sinasc.ESCMAE.alias('mother_years_of_study'), \
#		df_sinasc.ESTCIVMAE.alias('mother_marital_status'), \
#		df_sinasc.IDADEMAE.cast(IntegerType()).alias('mother_age'), \
#		df_sinasc.CODMUNNATU.cast(IntegerType()).alias('mother_city_of_birth'), \
#		df_sinasc.RACACORMAE.alias('mother_race'), \
#		df_sinasc.SERIESCMAE.cast(IntegerType()).alias('mother_elementary_school'), \
#		df_sinasc.IDADEPAI.cast(IntegerType()).alias('father_age'), \
#		df_sinasc.LOCNASC.alias('place_of_birth_type_source_value'), \
#		df_sinasc.CODESTAB.cast(IntegerType()).alias('care_site_of_birth_source_value'), \
#		df_sinasc.CODOCUPMAE.alias('mother_professional_occupation'), \
#		df_sinasc.NATURALMAE.cast(IntegerType()).alias('mother_country_of_origin'), \
#		df_sinasc.QTDFILMORT.cast(IntegerType()).alias('number_of_dead_children'), \
#		df_sinasc.QTDFILVIVO.cast(IntegerType()).alias('number_of_living_children'), \
#		df_sinasc.QTDGESTANT.cast(IntegerType()).alias('number_of_previous_pregnancies'), \
#		df_sinasc.QTDPARTCES.cast(IntegerType()).alias('number_of_previous_cesareans'), \
#		df_sinasc.QTDPARTNOR.cast(IntegerType()).alias('number_of_previous_normal_born') \
#		).rdd, df_datasus_person_schema)	


#		df_sinasc.ESCMAE2010.alias('mother_education_level'), \
#		df_sinasc.ESCMAEAGR1.alias('mother_education_level_aggregated'), \
#		df_sinasc.CODUFNATU.cast(IntegerType()).alias('mother_state_of_birth'), \
#		df_sinasc.TPDOCRESP.cast(IntegerType()).alias('responsible_document_type'), \
#		df_sinasc.TPFUNCRESP.cast(IntegerType()).alias('responsible_role_type'), \

#campos = """
#df_sinasc.person_id.alias('person_id'),
#FSql.lit(1).alias('system_source_id'),
#FSql.to_timestamp(FSql.lpad(df_sinasc.DTNASC,8,'0'), 'DDmmyyyy').alias('datasus_person_source_date'), \
#FSql.lit(ingestion_timestamp).alias('ingestion_timestamp'), \
#FSql.lit(source_filename).alias('source_file'), \
#df_sinasc.DTNASCMAE.alias('mother_birth_date_source_value'),
#FSql.to_date(FSql.lpad(df_sinasc.DTNASCMAE, 10, '0'), 'DDmmyyyy').alias('mother_birth_date'),
#df_sinasc.ESCMAE.alias('mother_years_of_study'),
#df_sinasc.ESTCIVMAE.alias('mother_marital_status'),
#df_sinasc.IDADEMAE.cast(IntegerType()).alias('mother_age'),
#df_sinasc.CODMUNNATU.cast(IntegerType()).alias('mother_city_of_birth'),
#df_sinasc.RACACORMAE.alias('mother_race'),
#df_sinasc.SERIESCMAE.cast(IntegerType()).alias('mother_elementary_school'),
#df_sinasc.IDADEPAI.cast(IntegerType()).alias('father_age'),
#df_sinasc.LOCNASC.alias('place_of_birth_type_source_value'),
#df_sinasc.CODESTAB.cast(IntegerType()).alias('care_site_of_birth_source_value'),
#df_sinasc.CODOCUPMAE.alias('mother_professional_occupation'),
#df_sinasc.NATURALMAE.cast(IntegerType()).alias('mother_country_of_origin'),
#df_sinasc.QTDFILMORT.cast(IntegerType()).alias('number_of_dead_children'),
#df_sinasc.QTDFILVIVO.cast(IntegerType()).alias('number_of_living_children'),
#df_sinasc.QTDGESTANT.cast(IntegerType()).alias('number_of_previous_pregnancies'),
#df_sinasc.QTDPARTCES.cast(IntegerType()).alias('number_of_previous_cesareans'),
#df_sinasc.QTDPARTNOR.cast(IntegerType()).alias('number_of_previous_normal_born')
#"""
#
#if any(field.name == "ESCMAE2010" for field in df_sinasc.schema.fields):
#	campos += ", df_sinasc.ESCMAE2010.alias('mother_education_level')"
#else:
#	campos += ", FSql.lit(None).cast(StringType()).alias('mother_education_level')"
#
#if any(field.name == "ESCMAEAGR1" for field in df_sinasc.schema.fields):
#	campos += ", df_sinasc.ESCMAEAGR1.alias('mother_education_level_aggregated')"
#else:
#	campos += ", FSql.lit(None).cast(StringType()).alias('mother_education_level_aggregated')"
#
#if any(field.name == "CODUFNATU" for field in df_sinasc.schema.fields):
#	campos += ", df_sinasc.CODUFNATU.cast(IntegerType()).alias('mother_state_of_birth')"
#else:
#	campos += ", FSql.lit(None).cast(IntegerType()).alias('mother_state_of_birth')"
#
#if any(field.name == "TPDOCRESP" for field in df_sinasc.schema.fields):
#	campos += ", df_sinasc.TPDOCRESP.cast(IntegerType()).alias('responsible_document_type')"
#else:
#	campos += ", FSql.lit(None).cast(IntegerType()).alias('responsible_document_type')"
#
#if any(field.name == "TPFUNCRESP" for field in df_sinasc.schema.fields):
#	campos += ", df_sinasc.TPFUNCRESP.cast(IntegerType()).alias('responsible_role_type')"
#else:
#	campos += ", FSql.lit(None).cast(IntegerType()).alias('responsible_role_type')"
#
#lista_campos = [FSql.expr(campo.strip()) for campo in campos.split(",")]
#
#df_datasus_person = spark.createDataFrame(
#	df_sinasc.select(*lista_campos).rdd,
#	df_datasus_person_schema
#)

###################################################################################

		campos = [
			{"expr": "df_sinasc.person_id", "alias": "person_id"},
			{"expr": "FSql.lit(1)", "alias": "system_source_id"},
			{"expr": "FSql.to_timestamp(FSql.lpad(df_sinasc.DTNASC, 8, '0'), 'ddMMyyyy')", "alias": "datasus_person_source_date"},
			{"expr": "FSql.lit(ingestion_timestamp)", "alias": "ingestion_timestamp"},
			{"expr": "FSql.lit(source_filename)", "alias": "source_file"},
			{"expr": "df_sinasc.DTNASCMAE", "alias": "mother_birth_date_source_value"},
			{"expr": "FSql.to_date(FSql.lpad(df_sinasc.DTNASCMAE, 8, '0'), 'ddMMyyyy')", "alias": "mother_birth_date"},
			{"expr": "df_sinasc.ESCMAE", "alias": "mother_years_of_study"},
			{"expr": "df_sinasc.ESTCIVMAE", "alias": "mother_marital_status"},
			{"expr": "df_sinasc.IDADEMAE.cast(IntegerType())", "alias": "mother_age"},
			{"expr": "df_sinasc.CODMUNNATU.cast(IntegerType())", "alias": "mother_city_of_birth"},
			{"expr": "df_sinasc.RACACORMAE", "alias": "mother_race"},
			{"expr": "df_sinasc.SERIESCMAE.cast(IntegerType())", "alias": "mother_elementary_school"},
			{"expr": "df_sinasc.IDADEPAI.cast(IntegerType())", "alias": "father_age"},
			{"expr": "df_sinasc.LOCNASC", "alias": "place_of_birth_type_source_value"},
			{"expr": "df_sinasc.CODESTAB.cast(IntegerType())", "alias": "care_site_of_birth_source_value"},
			{"expr": "df_sinasc.CODOCUPMAE", "alias": "mother_professional_occupation"},
			{"expr": "df_sinasc.NATURALMAE.cast(IntegerType())", "alias": "mother_country_of_origin"},
			{"expr": "df_sinasc.QTDFILMORT.cast(IntegerType())", "alias": "number_of_dead_children"},
			{"expr": "df_sinasc.QTDFILVIVO.cast(IntegerType())", "alias": "number_of_living_children"},
			{"expr": "df_sinasc.QTDGESTANT.cast(IntegerType())", "alias": "number_of_previous_pregnancies"},
			{"expr": "df_sinasc.QTDPARTCES.cast(IntegerType())", "alias": "number_of_previous_cesareans"},
			{"expr": "df_sinasc.QTDPARTNOR.cast(IntegerType())", "alias": "number_of_previous_normal_born"}
		]

		# Adicione as colunas condicionalmente
		if any(field.name == "ESCMAE2010" for field in df_sinasc.schema.fields):
			campos.append({"expr": "df_sinasc.ESCMAE2010", "alias": "mother_education_level"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "mother_education_level"})

		if any(field.name == "ESCMAEAGR1" for field in df_sinasc.schema.fields):
			campos.append({"expr": "df_sinasc.ESCMAEAGR1", "alias": "mother_education_level_aggregated"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "mother_education_level_aggregated"})

		if any(field.name == "CODUFNATU" for field in df_sinasc.schema.fields):
			campos.append({"expr": "df_sinasc.CODUFNATU.cast(IntegerType())", "alias": "mother_state_of_birth"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "mother_state_of_birth"})

		if any(field.name == "TPDOCRESP" for field in df_sinasc.schema.fields):
			campos.append({"expr": "df_sinasc.TPDOCRESP.cast(IntegerType())", "alias": "responsible_document_type"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "responsible_document_type"})

		if any(field.name == "TPFUNCRESP" for field in df_sinasc.schema.fields):
			campos.append({"expr": "df_sinasc.TPFUNCRESP.cast(IntegerType())", "alias": "responsible_role_type"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "responsible_role_type"})

		lista_campos = [eval(campo["expr"]).alias(campo["alias"]) for campo in campos]

		df_datasus_person = spark.createDataFrame(df_sinasc.select(*lista_campos).rdd, df_datasus_person_schema)

####################################################################################


		# persistindo os dados de observation_period no banco.
		if df_datasus_person.count() > 0:
			df_datasus_person.writeTo("bios.rebios.datasus_person").append()
			logger.info("Table Dataus Person was succesfully updated with SINASC data.")

		logger.info("ETL using SINASC data finished with success. Please, check the log file.")
		# return success
		sys.exit(0)
	except Exception as e:
		logger.error(f"Fatal error while loading data from SINASC: {str(e)}")
		#return failure
		sys.exit(-1)
 
if sys.argv[1] == 'CLIMATE':
	try:
		if num_args != 4:
			logger.error("Check the command line usage. For CLIMATE the options are as below.")
			logger.error("Usage: ")
			logger.error("   spark-submit loading.py CLIMATE /path_to_folder_with_climate_data/ file_name_with_climate_data")
			sys.exit(-1)

		logger.info("Loading external data from CLIMATE to Climaterna database.")

		df_climate = spark.read.parquet(os.path.join(sys.argv[2], sys.argv[3]))
		#df_climate.show()

		logger.info("Loading file: %s ", os.path.join(sys.argv[2], sys.argv[3]))

		logger.info("Loading info from Locations...")
		df_location = spark.read.format("iceberg").load(f"bios.rebios.location").filter(FSql.col("county").isNotNull())
		#df_location.show()

		df_climate = (df_climate.join(df_location, [df_climate.code_muni == df_location.location_source_value], 'left'))
		#df_climate.show()

		df_states = loadStates(spark, logger)
		#df_load = df_load.withColumn("COD_UF", FSql.substring("geocodigo", 1, 2))
		df_climate = (df_climate.join(df_states, [df_climate.county == df_states.codigo_uf], 'left'))
		#df_climate.show()

		logger.info("Writing input data to Climaterna table...")
		df_climate_schema = StructType([ \
		StructField("location_id", LongType(), True), \
		StructField("city_code", StringType(), True), \
		StructField("city_name", StringType(), True), \
		StructField("state_code", StringType(), True), \
		StructField("state_abbreviation", StringType(), True), \
		StructField("state_name", StringType(), True), \
		StructField("region_code", FloatType(), True), \
		StructField("region_name", StringType(), True), \
		StructField("measurement_date", StringType(), True), \
		StructField("temperature_min_mean_value", FloatType(), True), \
		StructField("temperature_min_min_value", FloatType(), True), \
		StructField("temperature_min_max_value", FloatType(), True), \
		StructField("temperature_min_stdev_value", FloatType(), True), \
		StructField("temperature_min_sum_value", FloatType(), True), \
		StructField("temperature_max_mean_value", FloatType(), True), \
		StructField("temperature_max_min_value", FloatType(), True), \
		StructField("temperature_max_max_value", FloatType(), True), \
		StructField("temperature_max_stdev_value", FloatType(), True), \
		StructField("temperature_max_sum_value", FloatType(), True), \
		StructField("precipitation_mean_value", FloatType(), True), \
		StructField("precipitation_min_value", FloatType(), True), \
		StructField("precipitation_max_value", FloatType(), True), \
		StructField("precipitation_stdev_value", FloatType(), True), \
		StructField("precipitation_sum_value", FloatType(), True), \
		StructField("relative_humidity_mean_value", FloatType(), True), \
		StructField("relative_humidity_min_value", FloatType(), True), \
		StructField("relative_humidity_max_value", FloatType(), True), \
		StructField("relative_humidity_stdev_value", FloatType(), True), \
		StructField("relative_humidity_sum_value", FloatType(), True), \
		StructField("solar_radiaton_mean_value", FloatType(), True), \
		StructField("solar_radiaton_min_value", FloatType(), True), \
		StructField("solar_radiaton_max_value", FloatType(), True), \
		StructField("solar_radiaton_stdev_value", FloatType(), True), \
		StructField("solar_radiaton_sum_value", FloatType(), True), \
		StructField("wind_speed_mean_value", FloatType(), True), \
		StructField("wind_speed_min_value", FloatType(), True), \
		StructField("wind_speed_max_value", FloatType(), True), \
		StructField("wind_speed_stdev_value", FloatType(), True), \
		StructField("wind_speed_sum_value", FloatType(), True), \
		StructField("evapotranspiration_mean_value", FloatType(), True), \
		StructField("evapotranspiration_min_value", FloatType(), True), \
		StructField("evapotranspiration_max_value", FloatType(), True), \
		StructField("evapotranspiration_stdev_value", FloatType(), True), \
		StructField("evapotranspiration_sum_value", FloatType(), True), \
		StructField("ethane_c2h6", FloatType(), True), \
		StructField("acetone_ch3coch3", FloatType(), True), \
		StructField("methane_ch4", FloatType(), True), \
		StructField("carbon_monoxide_co", FloatType(), True), \
		StructField("carbon_dioxide_co2s", FloatType(), True), \
		StructField("hydrogen_chloride_hcl", FloatType(), True), \
		StructField("max_to_min_concentration_rate_mmrpm10", FloatType(), True), \
		StructField("max_to_min_concentration_rate_mmrpm2p5", FloatType(), True), \
		StructField("nitrogen_dioxide_no2", FloatType(), True), \
		StructField("maximum_tropospheric_ozone_sfo3max", FloatType(), True), \
		StructField("sulfur_dioxide_so2", FloatType(), True) \
		])

		# *************************************************************
		#  DATASUS_PERSON - Persistência dos dados 
		#  Para cada registro do source será criado um único correspondente na tabela DATASUS_PERSON
		#  Source field: TPROBSON
		# *************************************************************
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.

		# Variavel Descricao Unidade
		# Tmin Temperatura Mınima ◦C
		# Tmax Temperatura Maxima ◦C
		# pr Precipitacao mm
		# RH Umidade Relativa %
		# Rs Radiacao Solar MJ/m2
		# u2 Velocidade do Vento m/s
		# ETo Evapotranspiracao mm

		if any(field.name == "name_state" for field in df_climate.schema.fields):
			df_climate_iceberg=spark.createDataFrame(df_climate.select( \
			df_climate.location_id.cast(LongType()).alias('location_id'), \
			df_climate.code_muni.alias('city_code'), \
			df_climate.city.alias('city_name'), \
			df_climate.code_state.alias('state_code'), \
			df_climate.abbrev_state.alias('state_abbreviation'), \
			df_climate.name_state.alias('state_name'), \
			df_climate.code_region.cast(FloatType()).alias('region_code'), \
			df_climate.name_region.alias('region_name'), \
			df_climate.date.alias('measurement_date'), \
			df_climate.TMIN_mean.alias('temperature_min_mean_value'), \
			df_climate.TMIN_min.alias('temperature_min_min_value'), \
			df_climate.TMIN_max.alias('temperature_min_max_value'), \
			df_climate.TMIN_stdev.alias('temperature_min_stdev_value'), \
			FSql.lit(None).alias('temperature_min_sum_value'), \
			df_climate.TMAX_mean.alias('temperature_max_mean_value'), \
			df_climate.TMAX_min.alias('temperature_max_min_value'), \
			df_climate.TMAX_max.alias('temperature_max_max_value'), \
			df_climate.TMAX_stdev.alias('temperature_max_stdev_value'), \
			FSql.lit(None).alias('temperature_max_sum_value'), \
			df_climate.PR_mean.alias('precipitation_mean_value'), \
			df_climate.PR_min.alias('precipitation_min_value'), \
			df_climate.PR_max.alias('precipitation_max_value'), \
			df_climate.PR_stdev.alias('precipitation_stdev_value'), \
			df_climate.PR_sum.alias('precipitation_sum_value'), \
			df_climate.RH_mean.alias('relative_humidity_mean_value'), \
			df_climate.RH_min.alias('relative_humidity_min_value'), \
			df_climate.RH_max.alias('relative_humidity_max_value'), \
			df_climate.RH_stdev.alias('relative_humidity_stdev_value'), \
			FSql.lit(None).alias('relative_humidity_sum_value'), \
			df_climate.Rs_mean.alias('solar_radiaton_mean_value'), \
			df_climate.Rs_min.alias('solar_radiaton_min_value'), \
			df_climate.Rs_max.alias('solar_radiaton_max_value'), \
			df_climate.Rs_stdev.alias('solar_radiaton_stdev_value'), \
			FSql.lit(None).alias('solar_radiaton_sum_value'), \
			df_climate.u2_mean.alias('wind_speed_mean_value'), \
			df_climate.u2_min.alias('wind_speed_min_value'), \
			df_climate.u2_max.alias('wind_speed_max_value'), \
			df_climate.u2_stdev.alias('wind_speed_stdev_value'), \
			FSql.lit(None).alias('wind_speed_sum_value'), \
			df_climate.ETo_mean.alias('evapotranspiration_mean_value'), \
			df_climate.ETo_min.alias('evapotranspiration_min_value'), \
			df_climate.ETo_max.alias('evapotranspiration_max_value'), \
			df_climate.ETo_stdev.alias('evapotranspiration_stdev_value'), \
			df_climate.ETo_sum.alias('evapotranspiration_sum_value'), \
			df_climate.c2h6.alias('ethane_c2h6'), \
			df_climate.ch3coch3.alias('acetone_ch3coch3'), \
			df_climate.ch4.alias('methane_ch4'), \
			df_climate.co2s.alias('carbon_dioxide_co2s'), \
			df_climate.co.alias('carbon_monoxide_co'), \
			df_climate.hcl.alias('hydrogen_chloride_hcl'), \
			df_climate.mmrpm10.alias('max_to_min_concentration_rate_mmrpm10'), \
			df_climate.mmrpm2p5.alias('max_to_min_concentration_rate_mmrpm2p5'), \
			df_climate.no2.alias('nitrogen_dioxide_no2'), \
			df_climate.sfo3max.alias('maximum_tropospheric_ozone_sfo3max'), \
			df_climate.so2.alias('sulfur_dioxide_so2') \
			).rdd, df_climate_schema)
		else:
			df_climate_iceberg=spark.createDataFrame(df_climate.select( \
			df_climate.location_id.cast(LongType()).alias('location_id'), \
			df_climate.code_muni.alias('city_code'), \
			df_climate.city.alias('city_name'), \
			df_climate.code_state.alias('state_code'), \
			df_climate.abbrev_state.alias('state_abbreviation'), \
			df_climate.nome_uf.alias('state_name'), \
			df_climate.codigo_regiao.cast(FloatType()).alias('region_code'), \
			df_climate.nome_regiao.alias('region_name'), \
			df_climate.date.alias('measurement_date'), \
			df_climate.TMIN_mean.alias('temperature_min_mean_value'), \
			df_climate.TMIN_min.alias('temperature_min_min_value'), \
			df_climate.TMIN_max.alias('temperature_min_max_value'), \
			df_climate.TMIN_stdev.alias('temperature_min_stdev_value'), \
			FSql.lit(None).alias('temperature_min_sum_value'), \
			df_climate.TMAX_mean.alias('temperature_max_mean_value'), \
			df_climate.TMAX_min.alias('temperature_max_min_value'), \
			df_climate.TMAX_max.alias('temperature_max_max_value'), \
			df_climate.TMAX_stdev.alias('temperature_max_stdev_value'), \
			FSql.lit(None).alias('temperature_max_sum_value'), \
			df_climate.PR_mean.alias('precipitation_mean_value'), \
			df_climate.PR_min.alias('precipitation_min_value'), \
			df_climate.PR_max.alias('precipitation_max_value'), \
			df_climate.PR_stdev.alias('precipitation_stdev_value'), \
			df_climate.PR_sum.alias('precipitation_sum_value'), \
			df_climate.RH_mean.alias('relative_humidity_mean_value'), \
			df_climate.RH_min.alias('relative_humidity_min_value'), \
			df_climate.RH_max.alias('relative_humidity_max_value'), \
			df_climate.RH_stdev.alias('relative_humidity_stdev_value'), \
			FSql.lit(None).alias('relative_humidity_sum_value'), \
			df_climate.Rs_mean.alias('solar_radiaton_mean_value'), \
			df_climate.Rs_min.alias('solar_radiaton_min_value'), \
			df_climate.Rs_max.alias('solar_radiaton_max_value'), \
			df_climate.Rs_stdev.alias('solar_radiaton_stdev_value'), \
			FSql.lit(None).alias('solar_radiaton_sum_value'), \
			df_climate.u2_mean.alias('wind_speed_mean_value'), \
			df_climate.u2_min.alias('wind_speed_min_value'), \
			df_climate.u2_max.alias('wind_speed_max_value'), \
			df_climate.u2_stdev.alias('wind_speed_stdev_value'), \
			FSql.lit(None).alias('wind_speed_sum_value'), \
			df_climate.ETo_mean.alias('evapotranspiration_mean_value'), \
			df_climate.ETo_min.alias('evapotranspiration_min_value'), \
			df_climate.ETo_max.alias('evapotranspiration_max_value'), \
			df_climate.ETo_stdev.alias('evapotranspiration_stdev_value'), \
			df_climate.ETo_sum.alias('evapotranspiration_sum_value'), \
			df_climate.c2h6.alias('ethane_c2h6'), \
			df_climate.ch3coch3.alias('acetone_ch3coch3'), \
			df_climate.ch4.alias('methane_ch4'), \
			df_climate.co2s.alias('carbon_dioxide_co2s'), \
			df_climate.co.alias('carbon_monoxide_co'), \
			df_climate.hcl.alias('hydrogen_chloride_hcl'), \
			df_climate.mmrpm10.alias('max_to_min_concentration_rate_mmrpm10'), \
			df_climate.mmrpm2p5.alias('max_to_min_concentration_rate_mmrpm2p5'), \
			df_climate.no2.alias('nitrogen_dioxide_no2'), \
			df_climate.sfo3max.alias('maximum_tropospheric_ozone_sfo3max'), \
			df_climate.so2.alias('sulfur_dioxide_so2') \
			).rdd, df_climate_schema)
			

		#df_climate_iceberg.show()

		# persistindo os dados de observation_period no banco.
		if df_climate_iceberg.count() > 0:
			df_climate_iceberg.writeTo("bios.rebios.datasus_climate").append()

		logger.info("CLIMATE data execution finished with success. Please, check the log file.")
		# return success
		sys.exit(0)

	except Exception as e:
		logger.error(f"Fatal error while loading data from SINASC: {str(e)}")
		#return failure
		sys.exit(-1)

if sys.argv[1] == 'SIM':
	try:
		if num_args != 4:
			logger.error("Check the command line usage. For SIM the options are as below.")
			logger.error("Usage: ")
			logger.error("   spark-submit loading.py SIM /path_to_folder_with_source_file source_file_name")
			sys.exit(-1)

		logger.info("Initiating SIM processing from source files to OMOP database.")
		####################################################################
		##  Leitura do arquivo de entrada (source)                        ##
		####################################################################

		#carga dos dados do parquet do SIM
		#source_path = os.getenv("CTRNA_SOURCE_SIM_PATH","/home/etl-rebios/")
		#arquivo_entrada = "SIM_2010_2022.parquet"

		# leitura do SIM original em formato parquet
		if not os.path.isfile(os.path.join(sys.argv[2], sys.argv[3])):
				logger.error("Arquivo SIM não localizado. Carga interrompida.")
				sys.exit(-1)

		logger.info("Loading file: %s ", os.path.join(sys.argv[2], sys.argv[3]))
		df_sim = spark.read.parquet(os.path.join(sys.argv[2], sys.argv[3]))
		df_sim.count()
		source_filename = os.path.join(sys.argv[2], sys.argv[3])
		ingestion_timestamp = datetime.now()

# CODESTAB	Código do estabelecimento
# ATESTADO	CIDs informados no atestado.
# ATESTANTE	Indica se o medico que assina atendeu o paciente 1: Sim 2: Substituto 3: IML 4: SVO 5: Outros
# CIRCOBITO	Tipo de morte violenta ou circunstâncias em que se deu a morte não natural. (1 – acidente; 2 – suicídio; 3 – homicídio; 4 – outros; 9 – ignorado)
# CIRURGIA	Realização de cirurgia. (1 – sim; 2 – não; 9 – ignorado)
# GESTACAO	Faixas de semanas de gestação (1 - Menos de 22 semanas; 2 - 22 a 27 semanas; 3 - 28 a 31 semanas; 4 - 32 a 36 semanas; 5 - 37 a 41 semanas; 6 - 42 e + semanas)
# GRAVIDEZ	Tipo de gravidez. (1 – única; 2 – dupla; 3 – tripla e mais; 9 – ignorada)
# OBITOGRAV	Óbito na gravidez. (1 – sim; 2 – não; 9 – ignorado)
# OBITOPARTO	Momento do óbito em relação ao parto. (1 - antes; 2– durante; 3–depois; 9– Ignorado)
# OBITOPUERP	Óbito no puerpério. (1 – Sim, até 42 dias após o parto; 2 – Sim, de 43 dias a 1 ano; 3 – Não; 9 – Ignorado)
# SEMAGESTAC	Numero de registro do óbito
# STCODIFICA	Status de instalação. (Se codificadora (valor: S) ou não (valor: N))
# STDOEPIDEM	Status de DO Epidemiológica. (1 - Sim; 0 - Não)
# STDONOVA	Status de DO Nova. (1 - Sim; 0 - Não)
# TIPOBITO	"Óbito fetal, morte antes da expulsão ou da extração completa do corpo da Mãe, independentemente da duração da gravidez. (1-Fetal; 2-Não Fetal)"
# TPMORTEOCO	Situação gestacional ou pósgestacional em que ocorreu o óbito. (1 – na gravidez; 2 – no parto; 3 – no abortamento; 4 – até 42 dias após o término do parto; 5 – de 43 dias a 1 ano após o término da gestação ; 8 – não ocorreu nestes períodos; 9 – ignorado)
# ACIDTRAB	Indica se o evento que desencadeou o óbito está relacionado ao processo de trabalho. (1 – sim; 2 – não; 9 – ignorado)
# ALTCAUSA	Indica se houve correção ou alteração da causa do óbito após investigação. (1- Sim; 2 – Não)
# ASSISTMED	Se refere ao atendimento médico continuado que o paciente recebeu, ou não, durante a enfermidade que ocasionou o óbito. (1 – sim; 2 – não; 9 – ignorado)
# CODBAIOCOR	codigo do bairro de ocorrencia
# CODMUNNATU	Código do município de naturalidade do falecido
# CODMUNOCOR	Código relativo ao município onde ocorreu o óbito.
# COMUNSVOIM	Código do município do SVO ou do IML
# DTCADINF	Quando preenchido indica se a investigação foi realizada. (Data no padrão ddmmaaaa)
# ESC	Escolaridade em anos. (1 – Nenhuma; 2 – de 1 a 3 anos; 3 – de 4 a 7 anos; 4 – de 8 a 11 anos; 5 – 12 anos e mais; 9 – Ignorado)
# ESC2010	Escolaridade 2010. Nível da última série concluída pelo falecido. (0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado)
# ESCFALAGR1	Escolaridade do falecido agregada (formulário a partir de 2010). (00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico)
# ESCMAE	Escolaridade da mãe em anos. (1 – Nenhuma; 2 – de 1 a 3 anos; 3 – de 4 a 7 anos; 4 – de 8 a 11 anos; 5 – 12 anos e mais; 9 – Ignorado)
# ESCMAE2010	Escolaridade 2010. Nível da última série concluída pela mãe. (0 – Sem escolaridade; 1 – Fundamental I (1ª a 4ª série); 2 – Fundamental II (5ª a 8ª série); 3 – Médio (antigo 2º Grau); 4 – Superior incompleto; 5 – Superior completo; 9 – Ignorado)
# ESCMAEAGR1	Escolaridade da mãe agregada (formulário a partir de 2010). (00 – Sem Escolaridade; 01 – Fundamental I Incompleto; 02 – Fundamental I Completo; 03 – Fundamental II Incompleto; 04 – Fundamental II Completo; 05 – Ensino Médio Incompleto; 06 – Ensino Médio Completo; 07 – Superior Incompleto; 08 – Superior Completo; 09 – Ignorado; 10 – Fundamental I Incompleto ou Inespecífico; 11 – Fundamental II Incompleto ou Inespecífico; 12 – Ensino Médio Incompleto ou Inespecífico)
# ESTCIV	Situação conjugal do falecido informada pelos familiares. (1 – Solteiro; 2 – Casado; 3 – Viúvo; 4 – Separado judicialmente/divorciado; 5 – União estável; 9 – Ignorado)
# IDADE	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: compostode dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1= minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idademenor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas:subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo variade 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# IDADEanos	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# IDADEdias	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# IDADEhoras	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# IDADEMAE	idade da mãe
# IDADEmeses	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# IDADEminutos	Idade do falecido em minutos, horas, dias, meses ou anos. (Idade: composto de dois subcampos. - O primeiro, de 1 dígito, indica a unidade da idade (se 1 = minuto, se 2 = hora, se 3 = mês, se 4 = ano, se = 5 idade maior que 100 anos). - O segundo, de dois dígitos, indica a quantidade de unidades: Idade menor de 1 hora: subcampo varia de 01 e 59 (minutos); De 1 a 23 Horas: subcampo varia de 01 a 23 (horas); De 24 horas e 29 dias: subcampo varia de 01 a 29 (dias); De 1 a menos de 12 meses completos: subcampo varia de 01 a 11 (meses); Anos - subcampo varia de 00 a 99; - 9 - ignorado)
# MORTEPARTO	Momento do óbito em relação ao parto. (1 - antes; 2– durante; 3–depois; 9– Ignorado)
# NUDIASOBCO	Diferença entre a data óbito e a data conclusão da investigação, em dias.
# NUMERODN	numero declaração nascidos vivos
# OCUP	Tipo de trabalho que o falecido desenvolveu na maior parte de sua vida produtiva.
# OCUPMAE	Trabalho exercido pela mãe
# QTDFILMORT	qtd filhos mortos (9-ignorado)
# QTDFILVIVO	número de filhos vivos (9-ignorado)
# SERIESCFAL	Última série escolar concluída pelo falecido. (Números de 1 a 8)
# SERIESCMAE	Última série escolar concluída pela mãe. (Números de 1 a 8)
# TPNIVELINV	Tipo de nível investigador. (E – estadual; R- regional; M- Municipal)
# TPOBITOCOR	Momento da ocorrência do óbito. (1-Durante a gestação, 2- Durante o abortamento, 3- Após o abortamento , 4- No parto ou até 1 hora após o parto, 5- No puerpério - até 42 dias após o parto, 6- Entre 43 dias e até 1 ano após o parto, 7- A investigação não identificou o momento do óbito, 8- Mais de um ano após o parto , 9- O óbito não ocorreu nas circunstancias anteriores, Branco - Não investigado)
# TPPOS	Óbito investigado. (1 – sim; 2 – não)
# TPRESGINFO	Informa se a investigação permitiu o resgate de alguma causa de óbito não informado, ou a correção de alguma antes informada. (01 - Não acrescentou 2014 (C 10) nem corrigiu informação; 02 - Sim, permitiu o resgate de novas informações; 03 - Sim, permitiu a correção de alguma das causas informadas originalmente)
# CAUSABAS	causa básica da DO
# CAUSABAS_O	Causa básica informada antes da resseleção.
# CAUSAMAT	CID da causa externa associada a uma causa materna.
# CB_PRE	Causa básica informada antes da resseleção (localidade)
# CRM	Número de inscrição do Médico atestante no Conselho Regional de Medicina
# HORAOBITO	Horário do óbito (padrão 24h 00:00)
# CODBAIRES	Código do Bairro de residência
# PESO	peso em gramas ao nascer
# DTATESTADO	data do atestado
# DTCADASTRO	Data do cadastro do óbito. ddmmaaaa
# UFINFORM	Código da UF que informou o registro
# CODMUNRES	Código do município de residência
# DTNASC	Data do nascimento do falecido
# LOCOCOR	Local de ocorrência do óbito. (1 – hospital; 2 – outros estabelecimentos de saúde; 3 – domicílio; 4 – via pública; 5 – outros; 6 - aldeia indígena; 9 – ignorado).
# NATURAL	País e Unidade da Federação onde falecido nasceu. Se estrangeiro informar País. (Números)
# ORIGEM	Origem do registro. (1- Oracle; 2 - Banco estadual diponibilizado via FTP; 3 - Banco SEADE; 9 - Ignorado)
# RACACOR	Cor informada pelo responsável pelas informações do falecido. (1 – Branca; 2 – Preta; 3 – Amarela; 4 – Parda; 5 – Indígena)
# SEXO	Sexo do falecido (M – masculino; F – feminino; I - ignorado) 
# EXAME	Realização de exame. (1 – sim; 2 – não; 9 – ignorado)
# LINHAA	CIDs informados na Linha A da DO referente ao diagnóstico na Linha A da DO (causa terminal - doença ou estado mórbido que causou diretamente a morte). (Códigos CID 10)
# LINHAB	CIDs informados na Linha B da DO referente ao diagnóstico na Linha B da DO (causa antecedente ou conseqüencial - estado mórbido, se existir, que produziu a causa direta da morte registrada na linha A)
# LINHAC	CIDs informados na Linha C da DO referente ao diagnóstico na Linha C da DO (causa antecedente ou conseqüencial - estado mórbido, se existir, que produziu a causa direta da morte registrada na linha A).
# LINHAD	CIDs informados na Linha D da DO referente ao diagnóstico na Linha D da DO (causa básica – estado mórbido, se existir, que produziu a causa direta da morte registrada na linha A).
# LINHAII	CIDs informados na Parte II da DO referente ao diagnóstico na Parte II da DO (causa contribuinte - outras condições significativas que contribuíram para a morte e que não entraram na cadeia definida na Parte I
# NECROPSIA	realização de necropsia (1 - sim;2-não;9-ignorado)
# NUDIASOBIN	
# PARTO	Tipo de parto. (1 – vaginal; 2 – cesáreo; 9 – ignorado)
# FONTE	fonte de informação utilizada para o preenchimento dos campos 48 e 49. (1 – ocorrência policial; 2 – hospital; 3 – família; 4 – outra; 9 – ignorado)
# FONTEINV	Fonte de investigação. (1 – Comitê de Morte Materna e/ou Infantil; 2 – Visita domiciliar / Entrevista família; 3 – Estabelecimento de Saúde / Prontuário; 4 – Relacionado com outros bancos de dados; 5 – S V O; 6 – I M L; 7 – Outra fonte; 8 – Múltiplas fontes; 9 – Ignorado)
# DTCADINV	Data do cadastro de investigação. (Data no padrão ddmmaaaa)
# DTCONCASO	Data de conclusão do caso
# DTCONINV	Data da conclusão da investigação
# DTINVESTIG	Data da investigação do óbito. ddmmaaaa
# DTOBITO	Data que ocorreu o obito (DDMMAAAA)

		# column HORANASC is presenting null values on source file and that will be filled with 0000
		df_sim=df_sim.fillna({"HORAOBITO": "0000"})

		# column DATANASC is presenting null values on source file and those records will be discarded
		df_sim=df_sim.filter((df_sim["DTOBITO"].isNull() == False) & (df_sim["DTOBITO"].rlike("^[0-9]{8}$")))
		#df_sim=df_sim.filter((df_sim["HORANASC"].isNull() == False))
		#df_sim=df_sim.filter((df_sim["HORANASC"].rlike("^[0-9]{4}$")) & (~df_sim["HORANASC"].rlike("[^0-9]")))
		#df_sim=df_sim.filter((df_sim["HORANASC"] <= 2359))
		df_sim = df_sim.withColumn("HORAOBITO", FSql.when(((df_sim["HORAOBITO"] > 2359) | ~(df_sim["HORAOBITO"].rlike("^[0-9]{4}$")) | (~df_sim["HORAOBITO"].rlike("[^0-9]"))) == True, "0000").otherwise(df_sim["HORAOBITO"]))


        # |-- LOCNASC <- as.factor(.data$LOCNASC): string (nullable = true)
        # |-- IDADEMAE <- as.factor(.data$IDADEMAE): string (nullable = true)
        # |-- ESTCIVMAE <- as.factor(.data$ESTCIVMAE): string (nullable = true)
        # |-- ESCMAE <- as.factor(.data$ESCMAE): string (nullable = true)
        # |-- CODOCUPMAE: string (nullable = true)
        # |-- GESTACAO <- as.factor(.data$GESTACAO): string (nullable = true)
        # |-- PARTO <- as.factor(.data$PARTO): string (nullable = true)


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

        # dataframe with records of Type Of Health Unit
		#df_cnes_tpunid = spark.read.format("iceberg").load(f"bios.rebios.type_of_unit")
		# dataframe with existing location records representing only cities
		logger.info("Loading info from Locations...")
		df_location = spark.read.format("iceberg").load(f"bios.rebios.location").filter(FSql.col("county").isNotNull())
		# dataframe with existing care_sites
		logger.info("Loading info from Care Sites...")
		df_care_site = spark.read.format("iceberg").load(f"bios.rebios.care_site")
		# dataframe with existing providers
		logger.info("Loading info from Providers...")
		df_provider = spark.read.format("iceberg").load(f"bios.rebios.provider")

		# dataframe with concept records
		#df_concept = spark.read.format("iceberg").load(f"bios.rebios.concept")

		#obtem o max person_id para usar na inserção de novos registros
		count_max_person_df = spark.sql("SELECT greatest(max(person_id),0) + 1 AS max_person FROM bios.rebios.person")
		count_max_person = count_max_person_df.first().max_person
		#geração dos id's únicos nos dados de entrada. O valor inicial é 0.
		# a função monotonically_increasing_id() gera números incrementais com a garantia de ser sempre maior que os existentes.
		df_sim = df_sim.withColumn("person_id", monotonically_increasing_id())
		#sincroniza os id's gerados com o max(person_id) existente no banco de dados atualizando os registros no df antes de escrever no banco
		df_sim = df_sim.withColumn("person_id", df_sim["person_id"] + count_max_person)

		# esses df's poderão conter valores nulos para município e estabelecimento de saúde, caso não haja cadastro.
		# a partir da coluna person_id, os registros de entrada se tornam unicamente identificados.
		# left outer join entre SIM e location para associar dados de município
		#df_sim_location = (df_sim.join(df_location, on=['df_sim.CODMUNRES == df_location.location_id'], how='left'))
		df_sim = (df_sim.join(df_location, [df_sim.CODMUNRES == df_location.location_source_value.substr(1, 6)], 'left'))
		df_sim = df_sim.withColumnRenamed("location_id","location_id_city")
		# left outer join entre SIM e care site para associar dados de estabelecimento de saúde
		df_sim = (df_sim.join(df_care_site, [df_sim.CODESTAB == df_care_site.care_site_source_value], 'left'))
		df_sim = df_sim.withColumnRenamed("location_id","location_id_care_site")

		#####  CRIAR O DF PARA CONTENDO O PERSON_ID E O CID10 CORRESPONDENTE
		# left outer join entre SIM e vocabulário para associar dados de 
		#df_sim_cid10 = (df_sim.join(df_concept, on=['df_sim.codmunres == df_concept.location_id'], how='left'))

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
			StructField("birth_timestamp", TimestampType(), True), \
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

		logger.info("Processing Person data...")
		df_person = spark.createDataFrame(df_sim.select(
			FSql.col("person_id"),
			FSql.when((FSql.col("SEXO") == 'M') | (FSql.col("SEXO") == '1') | (FSql.col("SEXO") == 'Masculino'), 8507)
			.when((FSql.col("SEXO") == 'F') | (FSql.col("SEXO") == '2') | (FSql.col("SEXO") == 'Feminino'), 8532)
			.otherwise(8551).alias("gender_concept_id"),
			FSql.coalesce(FSql.year(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lit('0000')), 'ddMMyyyy HHmm')), FSql.lit('0000').cast(IntegerType())).alias("year_of_birth"),
			FSql.lit(None).cast(IntegerType()).alias("month_of_birth"),
			FSql.lit(None).cast(IntegerType()).alias("day_of_birth"),
			FSql.to_timestamp(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTNASC"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), 'ddMMyyyy HHmm')).alias("birth_timestamp"),
			FSql.when ((FSql.col("RACACOR") == 1) | (FSql.col("RACACOR") == 'Branca'), 3212942)
			.when((FSql.col("RACACOR") == 2) | (FSql.col("RACACOR") == 'Preta'), 3213733)
			.when((FSql.col("RACACOR") == 3) | (FSql.col("RACACOR") == 'Amarela'), 3213498)
			.when((FSql.col("RACACOR") == 4) | (FSql.col("RACACOR") == 'Parda'), 3213487)
			.when((FSql.col("RACACOR") == 5) | (FSql.col("RACACOR") == 'Indígena'), 3213694)
			.otherwise(763013).alias("race_concept_id"),
			FSql.lit(38003563).alias('ethnicity_concept_id'),
			FSql.col("location_id_city").alias('location_id'),
			FSql.lit(None).cast(StringType()).alias('provider_id'),
			FSql.col("care_site_id").alias('care_site_id'),
			FSql.lit(None).cast(StringType()).alias('person_source_value'),
			FSql.col("SEXO").alias('gender_source_value'),
			FSql.lit(None).cast(StringType()).alias('gender_source_concept_id'),
			FSql.coalesce(FSql.col("RACACOR"), FSql.lit('Nulo')).alias('race_source_value'),
			FSql.lit(None).cast(StringType()).alias('race_source_concept_id'),
			FSql.lit(None).cast(StringType()).alias('ethnicity_source_value'),
			FSql.lit(None).cast(StringType()).alias('ethnicity_source_concept_id')
		).rdd, df_person_schema)


		if df_person.count() > 0:
			# Persistindo os dados de person no banco.
			df_person.show()
			df_person.writeTo("bios.rebios.person").append()
			logger.info("Table Person was succesfully updated with SIM data.")
		else:
			logger.error("Error on processing Person data.")
			exit(-1)

		df_death_schema = StructType([ \
			StructField("person_id", LongType(), False), \
			StructField("death_date", TimestampType(), False), \
			StructField("death_timestamp", TimestampType(), True), \
			StructField("death_type_concept_id", LongType(), True), \
			StructField("cause_concept_id", LongType(), True), \
			StructField("cause_source_value", StringType(), True), \
			StructField("cause_source_concept_id", LongType(), True) \
		])

		logger.info("Processing Death data...")
		df_person = spark.createDataFrame(df_sim.select(
			FSql.col("person_id"),
			FSql.to_timestamp(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORAOBITO"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')).alias("death_date"),
			FSql.to_timestamp(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.coalesce(FSql.col("HORAOBITO"), FSql.lit('0000')), 4, '0')), 'ddMMyyyy HHmm')).alias("death_timestamp"),
			FSql.lit(4306655).cast(LongType()).alias('death_type_concept_id'),
			FSql.lit(None).cast(LongType()).alias('cause_concept_id'),
			FSql.col("CAUSABAS").cast(StringType()).alias('cause_source_value'),
			FSql.lit(None).cast(LongType()).alias('cause_source_concept_id')
		).rdd, df_death_schema)

		if df_person.count() > 0:
			# Persistindo os dados de person no banco.
			df_person.show()
			df_person.writeTo("bios.rebios.death").append()
			logger.info("Table Death was succesfully updated with SIM data.")
		else:
			logger.error("Error on processing Death data.")
			exit(-1)

		df_datasus_death_schema = StructType([ \
			StructField("person_id", LongType(), False), \
			StructField("system_source_id", IntegerType(), False), \
			StructField("datasus_person_source_date", TimestampType(), False), \
			StructField("ingestion_timestamp", TimestampType(), False), \
			StructField("source_file", StringType(), False), \
			StructField("defunct_job_related_death", IntegerType(), True), \
			StructField("defunct_in_continuous_treatment", IntegerType(), True), \
			StructField("defunct_city_of_death", IntegerType(), True), \
			StructField("death_investigation_date", TimestampType(), True), \
			StructField("defunct_years_of_study", StringType(), True), \
			StructField("mother_years_of_study", StringType(), True), \
			StructField("defunct_marital_status", StringType(), True), \
			StructField("defunct_age", IntegerType(), True), \
			StructField("mother_age", IntegerType(), True), \
			StructField("birth_death", IntegerType(), True), \
			StructField("defunct_main_occupation", IntegerType(), True), \
			StructField("mother_professional_occupation", StringType(), True), \
			StructField("number_of_dead_children", IntegerType(), True), \
			StructField("number_of_living_children", IntegerType(), True), \
			StructField("death_occurrence_time", IntegerType(), True), \
			StructField("death_investigated", StringType(), True), \
			StructField("investigation_register_date", TimestampType(), True), \
			StructField("investigation_date", TimestampType(), True), \
			StructField("type_place_of_death", IntegerType(), True), \
			StructField("death_reason_changed", IntegerType(), True), \
			StructField("defunct_city_of_birth", IntegerType(), True), \
			StructField("autopsy_city", IntegerType(), True), \
			StructField("defunct_education_level", StringType(), True), \
			StructField("defunct_education_level_aggregated", StringType(), True), \
			StructField("mother_education_level", StringType(), True), \
			StructField("mother_education_level_aggregated", StringType(), True), \
			StructField("defunct_elementary_school", IntegerType(), True), \
			StructField("mother_elementary_school", IntegerType(), True), \
			StructField("death_investigation_level", IntegerType(), True), \
			StructField("death_additional_info", IntegerType(), True), \
			StructField("information_source", StringType(), True), \
			StructField("investigation_source", IntegerType(), True), \
			StructField("investigation_case_conclusion_date", TimestampType(), True), \
			StructField("investigation_conclusion_date", TimestampType(), True), \
			StructField("death_occurrence_time_2012", IntegerType(), True), \
			StructField("death_cause_original", StringType(), True), \
			StructField("death_cause_previous", StringType(), True), \
			StructField("physician_id", StringType(), True) \
			\
		])

		campos = [
			{"expr": "df_sim.person_id", "alias": "person_id"},
			{"expr": "FSql.lit(2)", "alias": "system_source_id"},
			{"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTOBITO, 8, '0'), 'ddMMyyyy')", "alias": "datasus_person_source_date"},
			{"expr": "FSql.lit(ingestion_timestamp)", "alias": "ingestion_timestamp"},
			{"expr": "FSql.lit(source_filename)", "alias": "source_file"},
			{"expr": "df_sim.ACIDTRAB.cast(IntegerType())", "alias": "defunct_job_related_death"},
			{"expr": "df_sim.ASSISTMED.cast(IntegerType())", "alias": "defunct_in_continuous_treatment"},
			{"expr": "df_sim.CODMUNOCOR.cast(IntegerType())", "alias": "defunct_city_of_death"},
			{"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTCADINF, 8, '0'), 'ddMMyyyy')", "alias": "death_investigation_date"},
			{"expr": "df_sim.ESC", "alias": "defunct_years_of_study"},
			{"expr": "df_sim.ESCMAE", "alias": "mother_years_of_study"},
			{"expr": "df_sim.ESTCIV", "alias": "defunct_marital_status"},
			{"expr": "df_sim.IDADE.cast(IntegerType())", "alias": "defunct_age"},
			{"expr": "df_sim.IDADEMAE.cast(IntegerType())", "alias": "mother_age"},
			{"expr": "df_sim.MORTEPARTO.cast(IntegerType())", "alias": "birth_death"},
			{"expr": "df_sim.OCUP.cast(IntegerType())", "alias": "defunct_main_occupation"},
			{"expr": "df_sim.OCUPMAE", "alias": "mother_professional_occupation"},
			{"expr": "df_sim.QTDFILMORT.cast(IntegerType())", "alias": "number_of_dead_children"},
			{"expr": "df_sim.QTDFILVIVO.cast(IntegerType())", "alias": "number_of_living_children"},
			{"expr": "df_sim.TPOBITOCOR.cast(IntegerType())", "alias": "death_occurrence_time"},
			{"expr": "df_sim.TPPOS", "alias": "death_investigated"},
			{"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTCADINV, 8, '0'), 'ddMMyyyy')", "alias": "investigation_register_date"},
			{"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTINVESTIG, 8, '0'), 'ddMMyyyy')", "alias": "investigation_date"},
			{"expr": "df_sim.LOCOCOR.cast(IntegerType())", "alias": "type_place_of_death"}
		]

		if any(field.name == "ALTCAUSA" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.ALTCAUSA.cast(IntegerType())", "alias": "death_reason_changed"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "death_reason_changed"})

		if any(field.name == "CODMUNNATU" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.CODMUNNATU.cast(IntegerType())", "alias": "defunct_city_of_birth"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "defunct_city_of_birth"})

		if any(field.name == "COMUNSVOIM" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.COMUNSVOIM.cast(IntegerType())", "alias": "autopsy_city"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "autopsy_city"})

		if any(field.name == "ESC2010" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.ESC2010", "alias": "defunct_education_level"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "defunct_education_level"})

		if any(field.name == "ESCFALAGR1" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.ESCFALAGR1", "alias": "defunct_education_level_aggregated"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "defunct_education_level_aggregated"})

		if any(field.name == "ESCMAE2010" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.ESCMAE2010", "alias": "mother_education_level"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "mother_education_level"})

		if any(field.name == "ESCMAEAGR1" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.ESCMAEAGR1", "alias": "mother_education_level_aggregated"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "mother_education_level_aggregated"})

		if any(field.name == "SERIESCFAL" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.SERIESCFAL.cast(IntegerType())", "alias": "defunct_elementary_school"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "defunct_elementary_school"})

		if any(field.name == "SERIESCMAE" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.SERIESCMAE.cast(IntegerType())", "alias": "mother_elementary_school"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "mother_elementary_school"})

		if any(field.name == "TPNIVELINV" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.TPNIVELINV.cast(IntegerType())", "alias": "death_investigation_level"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "death_investigation_level"})

		if any(field.name == "TPRESGINFO" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.TPRESGINFO.cast(IntegerType())", "alias": "death_additional_info"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "death_additional_info"})

		if any(field.name == "FONTE" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.FONTE", "alias": "information_source"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "information_source"})

		if any(field.name == "FONTEINV" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.FONTEINV.cast(IntegerType())", "alias": "investigation_source"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "investigation_source"})

		if any(field.name == "DTCONCASO" for field in df_sim.schema.fields):
			campos.append({"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTCONCASO, 8, '0'), 'ddMMyyyy')", "alias": "investigation_case_conclusion_date"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(TimestampType())", "alias": "investigation_case_conclusion_date"})

		if any(field.name == "DTCONINV" for field in df_sim.schema.fields):
			campos.append({"expr": "FSql.to_timestamp(FSql.lpad(df_sim.DTCONINV, 8, '0'), 'ddMMyyyy')", "alias": "investigation_conclusion_date"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(TimestampType())", "alias": "investigation_conclusion_date"})

		if any(field.name == "TPMORTEOCO" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.TPMORTEOCO.cast(IntegerType())", "alias": "death_occurrence_time_2012"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(IntegerType())", "alias": "death_occurrence_time_2012"})

		if any(field.name == "CAUSABAS_O" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.CAUSABAS_O", "alias": "death_cause_original"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "death_cause_original"})

		if any(field.name == "CB_PRE" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.CB_PRE", "alias": "death_cause_previous"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "death_cause_previous"})

		if any(field.name == "CRM" for field in df_sim.schema.fields):
			campos.append({"expr": "df_sim.CRM", "alias": "physician_id"})
		else:
			campos.append({"expr": "FSql.lit(None).cast(StringType())", "alias": "physician_id"})

		lista_campos = [eval(campo["expr"]).alias(campo["alias"]) for campo in campos]

		df_datasus_death = spark.createDataFrame(df_sim.select(*lista_campos).rdd, df_datasus_death_schema)

		if df_datasus_death.count() > 0:
			df_datasus_death.writeTo("bios.rebios.datasus_death").append()
			logger.info("Table Datasus Death was succesfully updated with SIM data.")

		# *************************************************************
		#  CONDITION_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela condition_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: ATESTADO
		# *************************************************************
		#spark.sql("""insert into condition_occurrence(condition_occurrence_id,person_id,condition_concept_id,condition_start_date,condition_type_concept_id, condition_source_value)
		#values
		#(df_condition_occur.identity, df_sinasc.identity, when df_sinasc.stdnepidem = 1 then 9999999 else 999999, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.stdnepidem)""") # STDNEPIDEM	Status de DN Epidemiológica. Valores: 1 – SIM; 0 – NÃO.

		# Definindo o novo esquema para suportar valores nulos e não-nulos.
		df_cond_occur_schema = StructType([ \
		StructField("condition_occurrence_id", LongType(), False), \
		StructField("person_id", LongType(), False), \
		StructField("condition_concept_id", LongType(), False), \
		StructField("condition_start_date", DateType(), False), \
        StructField("condition_start_timestamp", TimestampType(), True), \
		StructField("condition_end_date", DateType(), True), \
        StructField("condition_end_timestamp", TimestampType(), True), \
		StructField("condition_type_concept_id", LongType(), False), \
		StructField("condition_status_concept_id", LongType(), True), \
		StructField("stop_reason", StringType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id", LongType(), True), \
		StructField("visit_detail_id", LongType(), True), \
		StructField("condition_source_value", StringType(), True), \
		StructField("condition_source_concept_id", LongType(), True), \
		StructField("condition_status_source_value", StringType(), True)
		])

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "ATESTADO" for field in df_sim.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(4146323).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.ATESTADO.alias('condition_source_value'),
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [ATESTADO] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.

		if any(field.name == "ATESTANTE" for field in df_sim.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(762988).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(StringType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.ATESTANTE.alias('condition_source_value'),
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [ATESTANTE] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4302017).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.CIRCOBITO.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [CIRCOBITO] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4183699).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.CIRURGIA.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [CIRURGIA] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(40533858).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.GESTACAO.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [GESTACAO] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(432969).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.GRAVIDEZ.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [GRAVIDEZ] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(45880429).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.OBITOGRAV.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [OBITOGRAV] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4028785).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.OBITOPARTO.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [OBITOPARTO] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4239459).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.OBITOPUERP.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [OBITOPUERP] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "SEMAGESTAC" for field in df_sim.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(44789950).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.SEMAGESTAC.alias('condition_source_value'),
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [SEMAGESTAC] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		# if any(field.name == "STCODIFICA" for field in df_sim.schema.fields):
		# 	df_cond_occur=spark.createDataFrame(df_sim.select( \
		# 	FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		# 	df_sim.person_id.alias('person_id'), \
		# 	FSql.when(df_sim['STCODIFICA'] == '1', 999999).otherwise(999998).alias('condition_concept_id'), \
		# 	FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		# 	FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		# 	FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		# 	FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		# 	FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		# 	FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		# 	FSql.lit(None).cast(LongType()).alias('provider_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		# 	df_sim.STCODIFICA.alias('condition_source_value'),
		# 	FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		# 	FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		# 	).rdd, df_cond_occur_schema)

		# 	if df_cond_occur.count() > 0:
		# 		#obtem o max da tabela para usar na inserção de novos registros
		# 		count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
		# 		count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
		# 		#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
		# 		# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
		# 		# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
		# 		df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
		# 		#sincroniza os id's gerados com o max(person_id) existente no banco de dados
		# 		df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
		# 		# persistindo os dados de observation_period no banco.
		# 		df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
		# 		logger.info("Table Condition Occurrence [STCODIFICA] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		if any(field.name == "STDOEPIDEM" for field in df_sim.schema.fields):
			df_cond_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(4146792).cast(LongType()).alias('condition_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.STDOEPIDEM.alias('condition_source_value'),
			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
			).rdd, df_cond_occur_schema)

			if df_cond_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
				# persistindo os dados de observation_period no banco.
				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
				logger.info("Table Condition Occurrence [STDOEPIDEM] was succesfully updated with SIM data.")

     	# # Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# # e aplicando o novo esquema ao DataFrame e copiando os dados.
		# if any(field.name == "STDONOVA" for field in df_sim.schema.fields):
		# 	df_cond_occur=spark.createDataFrame(df_sim.select( \
		# 	FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		# 	df_sim.person_id.alias('person_id'), \
		# 	FSql.when(df_sim['STDONOVA'] == '1', 999999).otherwise(999998).alias('condition_concept_id'), \
		# 	FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		# 	FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		# 	FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		# 	FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		# 	FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		# 	FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		# 	FSql.lit(None).cast(LongType()).alias('provider_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		# 	FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		# 	df_sim.STDONOVA.alias('condition_source_value'),
		# 	FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		# 	FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		# 	).rdd, df_cond_occur_schema)

		# 	if df_cond_occur.count() > 0:
		# 		#obtem o max da tabela para usar na inserção de novos registros
		# 		count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
		# 		count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
		# 		#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
		# 		# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
		# 		# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
		# 		df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
		# 		#sincroniza os id's gerados com o max(person_id) existente no banco de dados
		# 		df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
		# 		# persistindo os dados de observation_period no banco.
		# 		df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
		# 		logger.info("Table Condition Occurrence [STDONOVA] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_cond_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4079844).cast(LongType()).alias('condition_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('condition_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('stop_reason'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.TIPOBITO.alias('condition_source_value'),
		FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
		).rdd, df_cond_occur_schema)

		if df_cond_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
			count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
			# persistindo os dados de observation_period no banco.
			df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
			logger.info("Table Condition Occurrence [TIPOBITO] was succesfully updated with SIM data.")

     	# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
#		if any(field.name == "TPMORTEOCO" for field in df_sim.schema.fields):
#			df_cond_occur=spark.createDataFrame(df_sim.select( \
#			FSql.lit(0).cast(LongType()).alias('condition_occurrence_id'), \
#			df_sim.person_id.alias('person_id'), \
#			FSql.when(df_sim['TPMORTEOCO'] == '1', 999999).otherwise(999998).alias('condition_concept_id'), \
#			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_start_date"), \
#			FSql.lit(None).cast(TimestampType()).alias('condition_start_timestamp'), \
#			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("condition_end_date"), \
#			FSql.lit(None).cast(TimestampType()).alias('condition_end_timestamp'), \
#			FSql.lit(4216316).cast(LongType()).alias('condition_type_concept_id'), \
#			FSql.lit(None).cast(LongType()).alias('condition_status_concept_id'), \
#			FSql.lit(None).cast(StringType()).alias('stop_reason'), \
#			FSql.lit(None).cast(LongType()).alias('provider_id'), \
#			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
#			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
#			df_sim.TPMORTEOCO.alias('condition_source_value'),
#			FSql.lit(None).cast(LongType()).alias('condition_source_concept_id'), \
#			FSql.lit(None).cast(StringType()).alias('condition_status_source_value') \
#			).rdd, df_cond_occur_schema)
#
#			if df_cond_occur.count() > 0:
#				#obtem o max da tabela para usar na inserção de novos registros
#				count_max_cond_occur_df = spark.sql("SELECT greatest(max(condition_occurrence_id),0) + 1 AS max_cond_occur FROM bios.rebios.condition_occurrence")
#				count_max_cond_occur = count_max_cond_occur_df.first().max_cond_occur
#				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
#				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
#				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
#				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", monotonically_increasing_id())
#				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
#				df_cond_occur = df_cond_occur.withColumn("condition_occurrence_id", df_cond_occur["condition_occurrence_id"] + count_max_cond_occur)
#				# persistindo os dados de observation_period no banco.
#				df_cond_occur.writeTo("bios.rebios.condition_occurrence").append()
#				logger.info("Table Condition Occurrence [TPMORTEOCO] was succesfully updated with SIM data.")

		# *************************************************************
		#  MEASUREMENT - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela measurement, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: PESO
		# *************************************************************
		#spark.sql("""insert into measurement (measurement_id,person_id,measurement_concept_id,measurement_date,measurement_type_concept_id,   # usado type_concept  Government Report 4216316value_as_number,measurement_source_value)
		#values (
		#(df_measurement.identity, df_sinasc.identity, 4264825, makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.peso, df_sinasc.peso)""") # PESO	Peso ao nascer em gramas.
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_measurement_schema = StructType([ \
		StructField("measurement_id", LongType(), False), \
		StructField("person_id", LongType(), False), \
		StructField("measurement_concept_id", LongType(), False), \
		StructField("measurement_date", DateType(), False), \
		StructField("measurement_type_concept_id", LongType(), False), \
		StructField("value_as_number", FloatType(), True), \
		StructField("measurement_timestamp", TimestampType(), True), \
		StructField("measurement_time", TimestampType(), True), \
		StructField("operator_concept_id", LongType(), True), \
		StructField("value_as_concept_id", LongType(), True), \
		StructField("unit_concept_id", LongType(), True), \
		StructField("range_low", FloatType(), True), \
		StructField("range_high", FloatType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id", LongType(), True), \
		StructField("visit_detail_id", LongType(), True), \
		StructField("measurement_source_value", StringType(), True), \
		StructField("measurement_source_concept_id", LongType(), True), \
		StructField("unit_source_value", StringType(), True), \
		StructField("unit_source_concept_id", LongType(), True), \
		StructField("value_source_value", StringType(), True), \
		StructField("measurement_event_id", LongType(), True), \
		StructField("meas_event_field_concept_id", LongType(), True) \
		])

		df_measurement=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('measurement_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4264825).cast(LongType()).alias('measurement_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias('measurement_date'), \
		FSql.lit(4306655).cast(LongType()).alias('measurement_type_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('value_as_number'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_timestamp'), \
		FSql.lit(None).cast(TimestampType()).alias('measurement_time'), \
		FSql.lit(None).cast(LongType()).alias('operator_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('value_as_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('unit_concept_id'), \
		FSql.lit(None).cast(FloatType()).alias('range_low'), \
		FSql.lit(None).cast(FloatType()).alias('range_high'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.PESO.alias('measurement_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('unit_source_value'), \
		FSql.lit(None).cast(LongType()).alias('unit_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('value_source_value'), \
		FSql.lit(None).cast(LongType()).alias('measurement_event_id'), \
		FSql.lit(None).cast(LongType()).alias('meas_event_field_concept_id') \
		).rdd, df_measurement_schema)

		if df_measurement.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_measurement_df = spark.sql("SELECT greatest(max(measurement_id),0) + 1 AS max_measurement FROM bios.rebios.measurement")
			count_max_measurement = count_max_measurement_df.first().max_measurement
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_measurement = df_measurement.withColumn("measurement_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_measurement = df_measurement.withColumn("measurement_id", df_measurement["measurement_id"] + count_max_measurement)
			# persistindo os dados de observation_period no banco.
			df_measurement.writeTo("bios.rebios.measurement").append()
			logger.info("Table Measurement Occurrence [PESO] was succesfully updated with SIM data.")


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
			StructField("observation_period_start_date", TimestampType(), False), \
			StructField("observation_period_end_date", TimestampType(), False), \
			StructField("period_type_concept_id", LongType(), False) \
		])

		#FSql.to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias("observation_period_start_date"), \
		#FSql.to_timestamp(concat(FSql.lpad(df_sinasc.DTNASC,8,'0'), FSql.lit(' '), FSql.lpad(df_sinasc.HORANASC,4,'0')), 'ddMMyyyy kkmm').alias('observation_period_end_date'), \

		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		logger.info("Processing Observation Period data...")
#		df_obs_period=spark.createDataFrame(df_sim.select(\
#		FSql.lit(0).cast(LongType()).alias('observation_period_id'), \
#		df_sim.person_id.alias('person_id'), \
#		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm')).alias("observation_period_start_date"),\
#		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm')).alias("observation_period_end_date"),\
#		FSql.lit(4193440).alias('period_type_concept_id')).rdd, \
#		df_obs_period_schema)
#
#		df_obs_period=spark.createDataFrame(df_sim.select(FSql.lit(0).cast(LongType()).alias('observation_period_id'),FSql.lit(1).alias('person_id'),FSql.col("DTATESTADO").alias('source_observation_period_start_date'),FSql.to_timestamp(FSql.when(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), 'ddMMyyyy HHmm').isNotNull(),FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))).otherwise(FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm').alias("observation_period_start_date"),FSql.to_timestamp(FSql.when(FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), 'ddMMyyyy HHmm').isNotNull(),FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))).otherwise(FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm').alias("observation_period_end_date"),FSql.lit(4193440).alias('period_type_concept_id')))

		df_obs_period = spark.createDataFrame(df_sim.select(\
			FSql.lit(0).cast(LongType()).alias('observation_period_id'),\
			FSql.lit(1).alias('person_id'),\
			FSql.to_timestamp(\
				FSql.when(\
					FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), 'ddMMyyyy HHmm').isNotNull(),\
					FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))\
				).otherwise(\
					FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))\
				), 'ddMMyyyy HHmm'\
			).alias("observation_period_start_date"),\
			FSql.to_timestamp(\
				FSql.when(\
					FSql.to_timestamp(FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), 'ddMMyyyy HHmm').isNotNull(),\
					FSql.concat(FSql.lpad(FSql.col("DTATESTADO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))\
				).otherwise(\
					FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))\
				), 'ddMMyyyy HHmm'\
			).alias("observation_period_end_date"),\
			FSql.lit(4306655).alias('period_type_concept_id')).rdd, \
		df_obs_period_schema)

		#df_null = df_obs_period.filter(df_obs_period["observation_period_start_date"].isNull() == True)
		#df_null.show(n=999999, truncate=False)

		if df_obs_period.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_obs_period_df = spark.sql("SELECT greatest(max(observation_period_id),0) + 1 AS max_obs_period FROM bios.rebios.observation_period")
			count_max_obs_period = count_max_obs_period_df.first().max_obs_period
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_obs_period = df_obs_period.withColumn("observation_period_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_obs_period = df_obs_period.withColumn("observation_period_id", df_obs_period["observation_period_id"] + count_max_obs_period)
			# persistindo os dados de observation_period no banco.
			df_obs_period.writeTo("bios.rebios.observation_period").append()
			logger.info("Table Obervation Period with [DTATESTADO] was succesfully updated with SIM data.")

		logger.info("Processing Observation Period data...")
		df_obs_period=spark.createDataFrame(df_sim.select(\
		FSql.lit(0).cast(LongType()).alias('observation_period_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTCADASTRO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm')).alias("observation_period_start_date"),\
		FSql.to_timestamp(FSql.to_timestamp(FSql.coalesce(FSql.concat(FSql.lpad(FSql.col("DTCADASTRO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0')), FSql.concat(FSql.lpad(FSql.col("DTOBITO"), 8, '0'), FSql.lit(' '), FSql.lpad(FSql.lit('0000'), 4, '0'))), 'ddMMyyyy HHmm')).alias("observation_period_end_date"),\
		FSql.lit(4306655).alias('period_type_concept_id')).rdd, \
		df_obs_period_schema)

		#df_null = df_obs_period.filter(df_obs_period["observation_period_start_date"].isNull() == True)
		#df_null.show(n=999999, truncate=False)

		if df_obs_period.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_obs_period_df = spark.sql("SELECT greatest(max(observation_period_id),0) + 1 AS max_obs_period FROM bios.rebios.observation_period")
			count_max_obs_period = count_max_obs_period_df.first().max_obs_period
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_obs_period = df_obs_period.withColumn("observation_period_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_obs_period = df_obs_period.withColumn("observation_period_id", df_obs_period["observation_period_id"] + count_max_obs_period)
			# persistindo os dados de observation_period no banco.
			df_obs_period.writeTo("bios.rebios.observation_period").append()
			logger.info("Table Obervation Period with [DTCADASTRO] was succesfully updated with SIM data.")


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
		StructField("procedure_timestamp", TimestampType(), True), \
		StructField("procedure_end_date", DateType(), True), \
		StructField("procedure_end_timestamp", TimestampType(), True), \
		StructField("procedure_type_concept_id", LongType(), False), \
		StructField("modifier_concept_id", LongType(), True), \
		StructField("quantity", IntegerType(), True), \
		StructField("provider_id", LongType(), True), \
		StructField("visit_occurrence_id",  LongType(), True), \
		StructField("visit_detail_id",  LongType(), True), \
		StructField("procedure_source_value", StringType(), True), \
		StructField("procedure_source_concept_id", LongType(), True), \
		StructField("modifier_source_value",  StringType(), True) \
		])

		# *************************************************************
		#  PROCEDURE_OCCURRENCE - Persistência dos dados 
		#  A partir de um registro do source serão inseridos vários registros na tabela procedure_occurrence, por isso, o dataframe é recriado trocando o campo de entrada.
		#  Source field: EXAME
		# *************************************************************
		#spark.sql("""insert into procedure_occurrence(procedure_occurrence_id,person_id,procedure_concept_id,procedure_date,procedure_type_concept_id,procedure_source_value)
		#values (
		#(df_procedure_occurrence.identity, df_sinasc.identity, case when df_sinasc.parto = 1 then 999999 when df_sinasc.parto = 2 then 4015701 else 9999999), makedate(substr(df_sinasc.dtnasc, 5), substr(df_sinasc.dtnasc, 3, 2), substr(df_sinasc.dtnasc, 1, 2)), 4216316, df_sinasc.parto)""") # PARTO	Tipo de parto: 1– Vaginal; 2– Cesário; 9– Ignorado
		# Populando o dataframe com os regisros de entrada para consistir nulos e não-nulos
		# e aplicando o novo esquema ao DataFrame e copiando os dados.
		df_proc_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4059372).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.EXAME.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [EXAME] was succesfully updated with SIM data.")


		df_proc_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4083743).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.LINHAA.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit('LINHAA').cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [LINHAA] was succesfully updated with SIM data.")

		df_proc_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4083743).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.LINHAB.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit('LINHAB').cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [LINHAB] was succesfully updated with SIM data.")

		df_proc_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4083743).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.LINHAC.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit('LINHAC').cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [LINHAC] was succesfully updated with SIM data.")

		df_proc_occur=spark.createDataFrame(df_sim.select( \
		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
		df_sim.person_id.alias('person_id'), \
		FSql.lit(4083743).cast(LongType()).alias('procedure_concept_id'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
		FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
		FSql.lit(None).cast(LongType()).alias('provider_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
		df_sim.LINHAD.alias('procedure_source_value'), \
		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
		FSql.lit('LINHAD').cast(StringType()).alias('modifier_source_value') \
		).rdd, df_proc_occur_schema)

		if df_proc_occur.count() > 0:
			#obtem o max da tabela para usar na inserção de novos registros
			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
			# persistindo os dados de observation_period no banco.
			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
			logger.info("Table Procedure Occurrence [LINHAD] was succesfully updated with SIM data.")

		if any(field.name == "LINHAII" for field in df_sim.schema.fields):
			df_proc_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(4083743).cast(LongType()).alias('procedure_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.LINHAII.alias('procedure_source_value'), \
			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
			FSql.lit('LINHAII').cast(StringType()).alias('modifier_source_value') \
			).rdd, df_proc_occur_schema)

			if df_proc_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
				# persistindo os dados de observation_period no banco.
				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
				logger.info("Table Procedure Occurrence [LINHAII] was succesfully updated with SIM data.")


#		df_proc_occur=spark.createDataFrame(df_sim.select( \
#		FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
#		df_sim.person_id.alias('person_id'), \
#		FSql.lit(999992).cast(LongType()).alias('procedure_concept_id'), \
#		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
#		FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
#		FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
#		FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
#		FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
#		FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
#		FSql.lit(None).cast(IntegerType()).alias('quantity'), \
#		FSql.lit(None).cast(LongType()).alias('provider_id'), \
#		FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
#		FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
#		df_sim.CAUSABAS_O.alias('procedure_source_value'), \
#		FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
#		FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
#		).rdd, df_proc_occur_schema)
#
#		if df_proc_occur.count() > 0:
#			#obtem o max da tabela para usar na inserção de novos registros
#			count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
#			count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
#			#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
#			# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
#			# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
#			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
#			#sincroniza os id's gerados com o max(person_id) existente no banco de dados
#			df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
#			# persistindo os dados de observation_period no banco.
#			df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
#			logger.info("Table Procedure Occurrence [CAUSABAS_O] was succesfully updated with SIM data.")

		if any(field.name == "CAUSAMAT" for field in df_sim.schema.fields):
			df_proc_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(4244279).cast(LongType()).alias('procedure_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.CAUSAMAT.alias('procedure_source_value'), \
			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
			).rdd, df_proc_occur_schema)

			if df_proc_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
				# persistindo os dados de observation_period no banco.
				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
				logger.info("Table Procedure Occurrence [CAUSAMAT] was succesfully updated with SIM data.")

#		if any(field.name == "CB_PRE" for field in df_sim.schema.fields):
#			df_proc_occur=spark.createDataFrame(df_sim.select( \
#			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
#			df_sim.person_id.alias('person_id'), \
#			FSql.lit(999992).cast(LongType()).alias('procedure_concept_id'), \
#			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
#			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
#			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
#			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
#			FSql.lit(4216316).cast(LongType()).alias('procedure_type_concept_id'), \
#			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
#			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
#			FSql.lit(None).cast(LongType()).alias('provider_id'), \
#			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
#			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
#			df_sim.CB_PRE.alias('procedure_source_value'), \
#			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
#			FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
#			).rdd, df_proc_occur_schema)
#
#			if df_proc_occur.count() > 0:
#				#obtem o max da tabela para usar na inserção de novos registros
#				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
#				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
#				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
#				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
#				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
#				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
#				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
#				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
#				# persistindo os dados de observation_period no banco.
#				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
#				logger.info("Table Procedure Occurrence [CB_PRE] was succesfully updated with SIM data.")


		if any(field.name == "NECROPSIA" for field in df_sim.schema.fields):
			df_proc_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(37396511).cast(LongType()).alias('procedure_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.NECROPSIA.alias('procedure_source_value'), \
			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
			).rdd, df_proc_occur_schema)

			if df_proc_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
				# persistindo os dados de observation_period no banco.
				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
				logger.info("Table Procedure Occurrence [NECROPSIA] was succesfully updated with SIM data.")


		if any(field.name == "NUDIASOBIN" for field in df_sim.schema.fields):
			df_proc_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(40482950).cast(LongType()).alias('procedure_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.NUDIASOBIN.alias('procedure_source_value'), \
			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
			).rdd, df_proc_occur_schema)

			if df_proc_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
				# persistindo os dados de observation_period no banco.
				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
				logger.info("Table Procedure Occurrence [NUDIASOBIN] was succesfully updated with SIM data.")

		if any(field.name == "PARTO" for field in df_sim.schema.fields):
			df_proc_occur=spark.createDataFrame(df_sim.select( \
			FSql.lit(0).cast(LongType()).alias('procedure_occurrence_id'), \
			df_sim.person_id.alias('person_id'), \
			FSql.lit(3955907).cast(LongType()).alias('procedure_concept_id'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_timestamp'), \
			FSql.to_date(FSql.lpad(df_sim.DTOBITO,8,'0'), 'DDmmyyyy').alias("procedure_end_date"), \
			FSql.lit(None).cast(TimestampType()).alias('procedure_end_timestamp'), \
			FSql.lit(4306655).cast(LongType()).alias('procedure_type_concept_id'), \
			FSql.lit(None).cast(LongType()).alias('modifier_concept_id'), \
			FSql.lit(None).cast(IntegerType()).alias('quantity'), \
			FSql.lit(None).cast(LongType()).alias('provider_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_occurrence_id'), \
			FSql.lit(None).cast(LongType()).alias('visit_detail_id'), \
			df_sim.PARTO.alias('procedure_source_value'), \
			FSql.lit(None).cast(LongType()).alias('procedure_source_concept_id'), \
			FSql.lit(None).cast(StringType()).alias('modifier_source_value') \
			).rdd, df_proc_occur_schema)

			if df_proc_occur.count() > 0:
				#obtem o max da tabela para usar na inserção de novos registros
				count_max_proc_occur_df = spark.sql("SELECT greatest(max(procedure_occurrence_id),0) + 1 AS max_proc_occur FROM bios.rebios.procedure_occurrence")
				count_max_proc_occur = count_max_proc_occur_df.first().max_proc_occur
				#geração dos id's únicos nos dados de entrada. O valor inicial é 1.
				# a ordenação a seguir é necessária para a função row_number(). Existe a opção de usar a função monotonically_increasing_id, mas essa conflita com o uso 
				# do select max(person_id) já que os id's gerados por ela são números compostos pelo id da partição e da linha na tabela. 
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", monotonically_increasing_id())
				#sincroniza os id's gerados com o max(person_id) existente no banco de dados
				df_proc_occur = df_proc_occur.withColumn("procedure_occurrence_id", df_proc_occur["procedure_occurrence_id"] + count_max_proc_occur)
				# persistindo os dados de observation_period no banco.
				df_proc_occur.writeTo("bios.rebios.procedure_occurrence").append()
				logger.info("Table Procedure Occurrence [PARTO] was succesfully updated with SIM data.")


		logger.info("ETL using SIM data finished with success. Please, check the log file.")
		# return success
		sys.exit(0)
	except Exception as e:
		logger.error(f"Fatal error while loading data from SIM: {str(e)}")
		#return failure
		sys.exit(-1)
