#!/bin/bash
set -x
# Caminho da pasta onde os arquivos estão localizados
pasta="/home/etl-rebios/etl-files"

# Caminho para o script .sh que será executado
script="spark-submit"

# Loop através de todos os arquivos na pasta
for arquivo in "$pasta"/BR-DWGD*.parquet; do
  # Verifica se é um arquivo regular
  if [ -f "$arquivo" ]; then
    # Extrai o nome do arquivo
    nome_arquivo=$(basename "$arquivo")
    # Executa o script .sh com os parâmetros necessários
    "$script" "loading.py" "CLIMATE" "/home/etl-rebios/etl-files/" "$nome_arquivo"
  fi
done
set +x
