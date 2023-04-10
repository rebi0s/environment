docker compose -f ./spark/docker-compose.yaml down | true
docker compose -f ./spark/docker-compose.yaml up -d

docker exec -it spark-master /bin/bash