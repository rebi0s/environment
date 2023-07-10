docker compose -f ./iceberg-base/docker-compose.yaml down | true
docker compose -f ./iceberg-base/docker-compose.yaml up -d $1
