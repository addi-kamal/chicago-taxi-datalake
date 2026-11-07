# Superset 4.1.2 + driver Trino (exposition BI du datalake).
FROM apache/superset:4.1.2

USER root
RUN pip install --no-cache-dir "trino[sqlalchemy]==0.332.0" "psycopg2-binary==2.9.10"

COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py
COPY docker/superset/bootstrap.sh /app/bootstrap.sh
# Fins de ligne LF garanties même si le repo a été cloné sous Windows.
RUN sed -i 's/\r$//' /app/bootstrap.sh /app/pythonpath/superset_config.py \
 && chmod 0755 /app/bootstrap.sh

USER superset
CMD ["/app/bootstrap.sh"]
