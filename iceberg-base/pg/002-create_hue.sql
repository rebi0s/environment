DROP DATABASE IF EXISTS db_hue;
CREATE DATABASE db_hue;
UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_hue';
