docker compose -f ../superset/docker-compose-image-tag.yml                                                                                                                                                                                                                 down | true
docker compose -f ../superset/docker-compose-image-tag.yml up -d $1
