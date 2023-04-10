psql -c "GRANT CONNECT ON DATABASE db_iceberg TO icbergcat;"
psql db_iceberg -c "CREATE SCHEMA IF NOT EXISTS bios;"
psql db_iceberg -c "GRANT ALL privileges ON SCHEMA bios TO icbergcat;"
psql db_iceberg -c "GRANT ALL privileges ON SCHEMA public TO icbergcat;"
psql db_iceberg -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bios TO icbergcat;"
psql db_iceberg -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO icbergcat;"
psql db_iceberg -c "ALTER SCHEMA bios OWNER TO icbergcat;"