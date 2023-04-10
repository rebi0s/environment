FROM postgres:15

RUN apt-get -y update && \
    apt-get -y install postgresql-15-cron && \
    mkdir -p /etc/postgresql/config/


COPY ./pg/postgresql.conf /etc/postgresql/config/
COPY ./pg/001-create_iceberg.sql /docker-entrypoint-initdb.d/
COPY ./pg/005-create_users.sql   /docker-entrypoint-initdb.d/
COPY ./pg/010-grants_and_schemas.sh /docker-entrypoint-initdb.d/

CMD [ "postgres", "-c", "config_file=/etc/postgresql/config/postgresql.conf"]
