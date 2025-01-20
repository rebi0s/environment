# Instruções para Rodar o Script de Ingestão

## Preparar a Base de Dados

Para rodar o script de ingestão é necessário seguir os passos a seguir.

### 1. Criar a Estrutura das Tabelas

Criar a estrutura das tabelas na base Iceberg usando o arquivo `OMOPCDM_iceberg_5.4_ddl.sql` do repositório do projeto. O arquivo é executado através da linha de comando a seguir:

```bash
spark-submit loading.py INIT /OMOPCDM_iceberg_5.4_ddl.sql
```

### 2. Inserir o Vocabulário do OMOP

O OMOP fornece uma lista de arquivos CSV para popular a base de dados dos projetos que o utilizam. Os arquivos estão localizados no repositório do projeto em `vocabulario_OMOP/vocabulary_download_v5_4.zip`. Uma vez descompactado o arquivo zip, rodar o comando a seguir:

```bash
spark-submit loading.py VOCAB_OMOP /path_to_csv_files
```

### 3. Inserir os Dados Cadastrais do DATASUS

Para isso, obter os arquivos de municípios e estabelecimentos hospitalares na pasta `/datasus_cadastros` do repositório e rodar as instruções a seguir:

```bash
spark-submit loading.py DATASUS -city /path_to_folder_with_cities lim_municipio_a.csv
spark-submit loading.py DATASUS -care /path_to_folder_with_care_sites tbEstabelecimento202404.csv
```

## Iniciar a Carga dos Dados de Entrada

Em seguida a essa preparação, iniciar a carga dos dados de entrada do SINASC, SIM e CLIMA, com as instruções a seguir apontando para os respectivos arquivos.

### SINASC

```bash
spark-submit loading.py SINASC /path_to_folder_with_source_file source_file_name
```

### SIM

```bash
spark-submit loading.py SIM /path_to_folder_with_source_file source_file_name
```

### CLIMA

```bash
spark-submit loading.py CLIMATE /path_to_folder_with_climate_data/ file_name_with_climate_data
```

## Conclusão

Ao término desse processamento, a base de dados do projeto Climaterna estará preenchida com os dados da ingestão.