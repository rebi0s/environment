docker compose -f ./spark/docker-compose.yaml down | true
docker compose -f ./spark/docker-compose.yaml up -d $1

docker exec -it spark_master /bin/bash