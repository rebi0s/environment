mkdir -p ../observability/prometheus | true

docker compose -f ./observability/observability-docker-compose.yaml  down | true
docker compose -f ./observability/observability-docker-compose.yaml up -d
