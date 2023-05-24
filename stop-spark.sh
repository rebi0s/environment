docker compose -f ./spark/docker-compose.yaml down | true


CREATE TABLE bios.my_table (
id bigint,
data string,
category string)
USING iceberg
LOCATION 's3://bios'
PARTITIONED BY (category);