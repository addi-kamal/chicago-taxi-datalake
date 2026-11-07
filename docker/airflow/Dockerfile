# Image unique Airflow + Spark (driver local[*]) + Iceberg + client Trino.
# Toutes les versions sont figées ; aucun jar n'est téléchargé au runtime.
FROM apache/airflow:2.10.5-python3.11

ARG ICEBERG_VERSION=1.8.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
ARG MAVEN_REPO=https://repo1.maven.org/maven2

USER root

# --- Java 17 (runtime Spark) ------------------------------------------------------
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

# --- Jars Iceberg / S3 / JDBC, vérifiés par SHA-1 Maven ----------------------------
# hadoop-aws 3.3.4 + aws-java-sdk-bundle 1.12.262 = versions alignées sur le Hadoop
# embarqué par PySpark 3.5.x.
RUN set -eu; mkdir -p /opt/spark-jars; cd /opt/spark-jars; \
    for coord in \
      "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"; \
    do \
      jar="$(basename "$coord")"; \
      curl -fsSL --retry 5 -o "$jar" "${MAVEN_REPO}/${coord}"; \
      echo "$(curl -fsSL --retry 5 "${MAVEN_REPO}/${coord}.sha1" | cut -d' ' -f1)  ${jar}" | sha1sum -c -; \
    done; \
    chown -R 50000:0 /opt/spark-jars

USER 50000

# --- Dépendances Python (contraintes officielles Airflow 2.10.5 / Python 3.11) ------
COPY --chown=50000:0 docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt"

# --- Code du projet -------------------------------------------------------------------
ENV PROJECT_HOME=/opt/airflow/project \
    PYTHONPATH=/opt/airflow/project/src \
    PYSPARK_PYTHON=python \
    SPARK_JARS_DIR=/opt/spark-jars
WORKDIR /opt/airflow/project
COPY --chown=50000:0 pyproject.toml ./
COPY --chown=50000:0 src ./src
COPY --chown=50000:0 dags ./dags
COPY --chown=50000:0 seeds ./seeds
COPY --chown=50000:0 sql ./sql
COPY --chown=50000:0 scripts ./scripts
COPY --chown=50000:0 tests ./tests
WORKDIR /opt/airflow
