DROP DATABASE IF EXISTS db_iceberg;
CREATE DATABASE db_iceberg;
UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_iceberg';
