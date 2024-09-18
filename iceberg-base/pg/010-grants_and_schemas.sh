psql -c "GRANT CONNECT ON DATABASE db_iceberg TO icbergcat;"
psql -c "GRANT CONNECT ON DATABASE db_hue TO huerole;"

psql db_hue -c "GRANT ALL privileges ON SCHEMA public TO huerole;"
psql db_hue -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO huerole;"

psql db_iceberg -c "GRANT ALL privileges ON SCHEMA public TO icbergcat;"
psql db_iceberg -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO icbergcat;"

psql db_superset -c "GRANT ALL privileges ON SCHEMA public TO sperset;"
psql db_superset -c "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO sperset;"
