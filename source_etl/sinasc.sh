#!/bin/bash
set -x

export CTRNA_LOG_PATH=/home/etl-rebios/etl-log/

# Caminho da pasta onde os arquivos estão localizados
pasta="/home/etl-rebios/etl-files/SINASC_Por_Ano/"

# Caminho para o script .sh que será executado
script="spark-submit"

# Loop através de todos os arquivos na pasta
# Usar o comando find para listar todos os arquivos .parquet nas subpastas
for arquivo in $(find "$pasta" -type f -name "sinasc_*.parquet"); do
  # Verifica se é um arquivo regular
  if [ -f "$arquivo" ]; then
    # Extrai o nome do arquivo
    nome_arquivo=$(basename "$arquivo")
    caminho_completo=$(dirname "$arquivo")
    
    echo $nome_arquivo
    # Executa o script .sh com os parâmetros necessários
    "$script" "loading.py" "SINASC" "$caminho_completo" "$nome_arquivo"
  fi
done

set +x
