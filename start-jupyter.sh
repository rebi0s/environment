docker compose -f ./jupyter/docker-compose.yml down | true
docker compose -f ./jupyter/docker-compose.yml up -d $1
