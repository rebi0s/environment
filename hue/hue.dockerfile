FROM gethue/hue:latest

COPY --chown=hue:root --chmod=664 ./overwrite/sql_alchemy.py /usr/share/hue/desktop/libs/notebook/src/notebook/connectors
