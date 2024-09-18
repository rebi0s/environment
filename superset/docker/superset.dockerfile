FROM apache/superset

COPY --chown=root:root --chmod=644 ./docker/sqlalchemy_hive.py /usr/local/lib/python3.10/site-packages/pyhive/

COPY --chown=superset:superset --chmod=644 ./docker/hive.py /app/superset/db_engine_specs/
