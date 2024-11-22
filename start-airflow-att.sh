docker compose -f ./airflow/docker-compose.yaml down | true
docker compose -f ./airflow/docker-compose.yaml up $1

