DROP DATABASE IF EXISTS db_superset;
CREATE DATABASE db_superset;
UPDATE pg_database SET datallowconn = 'true' WHERE datname = 'db_superset';
