"""Factory unique de SparkSession (Iceberg + catalogue JDBC + S3).

- ``s3://`` est lu/écrit par Iceberg via ``S3FileIO`` (tables) ;
- ``s3a://`` est utilisé par Spark pour lire les CSV bruts de la landing zone ;
- ``fs.s3.impl`` est aussi mappé sur S3A pour les actions Iceberg qui listent le
  stockage via Hadoop (``remove_orphan_files``).
"""

from __future__ import annotations

import glob
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

from taxi_pipeline.config import Settings

log = logging.getLogger(__name__)


def spark_conf(settings: Settings) -> dict[str, str]:
    """Configuration Spark dérivée des settings (testable sans JVM)."""
    cat = f"spark.sql.catalog.{settings.iceberg_catalog_name}"
    jars = sorted(glob.glob(os.path.join(settings.spark_jars_dir, "*.jar")))
    conf: dict[str, str] = {
        # --- Iceberg ----------------------------------------------------------------
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"{cat}.uri": settings.iceberg_catalog_uri,
        f"{cat}.jdbc.user": settings.iceberg_catalog_user,
        f"{cat}.jdbc.password": settings.iceberg_catalog_password,
        # Schéma V1 du catalogue JDBC : valeur par défaut côté Trino (support des vues).
        f"{cat}.jdbc.schema-version": "V1",
        f"{cat}.warehouse": settings.iceberg_warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": settings.s3_endpoint,
        f"{cat}.s3.path-style-access": str(settings.s3_path_style).lower(),
        f"{cat}.s3.access-key-id": settings.s3_access_key,
        f"{cat}.s3.secret-access-key": settings.s3_secret_key,
        f"{cat}.client.region": settings.s3_region,
        "spark.sql.defaultCatalog": settings.iceberg_catalog_name,
        # --- S3A (landing CSV + listings Hadoop) -------------------------------------
        "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
        "spark.hadoop.fs.s3a.path.style.access": str(settings.s3_path_style).lower(),
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.s3_use_ssl).lower(),
        "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
        "spark.hadoop.fs.s3a.endpoint.region": settings.s3_region,
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # --- Exécution -----------------------------------------------------------------
        "spark.driver.memory": settings.spark_driver_memory,
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "false",
        # Valeur illisible -> null (au lieu d'une SparkUpgradeException) lors du parsing des dates.
        "spark.sql.legacy.timeParserPolicy": "CORRECTED",
        "spark.sql.shuffle.partitions": str(settings.spark_shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.ui.enabled": str(settings.spark_ui_enabled).lower(),
        "spark.ui.showConsoleProgress": "false",
    }
    if jars:
        conf["spark.jars"] = ",".join(jars)
    return conf


def build_spark(settings: Settings, app_name: str) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).master(settings.spark_master)
    for key, value in spark_conf(settings).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession démarrée : %s (master=%s)", app_name, settings.spark_master)
    return spark


@contextmanager
def spark_session(settings: Settings, app_name: str) -> Iterator[SparkSession]:
    """Une SparkSession par job, toujours arrêtée (libère la mémoire du worker)."""
    spark = build_spark(settings, app_name)
    try:
        yield spark
    finally:
        spark.stop()
        log.info("SparkSession arrêtée : %s", app_name)
