"""Initialisation idempotente de la plateforme (exécutée par le service `init`).

1. PostgreSQL : rôles + bases ``airflow``, ``iceberg_catalog``, ``superset`` ;
2. Stockage S3 : buckets ``bronze``, ``silver``, ``gold``.

Écrit en Python (et non en scripts shell montés) : portable Windows/Linux/macOS
et identique sur Ceph RGW (aucune dépendance à l'outil `mc`).

Usage : ``python -m taxi_pipeline.infra.bootstrap``
"""

from __future__ import annotations

import logging
import os
import sys
import time
from collections.abc import Callable
from typing import TypeVar

import psycopg2
from psycopg2 import sql

from taxi_pipeline.config import get_settings
from taxi_pipeline.storage import S3ObjectStore

log = logging.getLogger("bootstrap")
T = TypeVar("T")


def retry(what: str, fn: Callable[[], T], timeout_s: int = 180, every_s: float = 3.0) -> T:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            return fn()
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"{what} indisponible après {timeout_s}s : {exc}") from exc
            log.info("En attente de %s (%s)...", what, type(exc).__name__)
            time.sleep(every_s)


def _env(name: str, default: str) -> str:
    return os.environ.get(name) or default


def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Variable {name} absente : générer .env (scripts/generate_env.py)")
    return value


def bootstrap_postgres() -> None:
    def connect() -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=_env("POSTGRES_HOST", "postgres"),
            port=int(_env("POSTGRES_PORT", "5432")),
            user=_env("POSTGRES_USER", "postgres"),
            password=_secret("POSTGRES_PASSWORD"),
            dbname="postgres",
            connect_timeout=5,
        )

    conn = retry("PostgreSQL", connect)
    conn.autocommit = True  # CREATE DATABASE interdit en transaction
    databases = [
        (
            "airflow",
            _env("AIRFLOW_DB_USER", "airflow"),
            _secret("AIRFLOW_DB_PASSWORD"),
        ),
        (
            "iceberg_catalog",
            _env("ICEBERG_CATALOG_USER", "iceberg"),
            _secret("ICEBERG_CATALOG_PASSWORD"),
        ),
        (
            "superset",
            _env("SUPERSET_DB_USER", "superset"),
            _secret("SUPERSET_DB_PASSWORD"),
        ),
    ]
    with conn.cursor() as cur:
        for dbname, user, password in databases:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (user,))
            verb = "ALTER" if cur.fetchone() else "CREATE"
            cur.execute(
                sql.SQL(verb + " ROLE {} WITH LOGIN PASSWORD {}").format(
                    sql.Identifier(user), sql.Literal(password)
                )
            )
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if not cur.fetchone():
                cur.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(dbname), sql.Identifier(user)
                    )
                )
                log.info("Base créée : %s (owner %s)", dbname, user)
            else:
                log.info("Base existante : %s", dbname)
    conn.close()


def bootstrap_buckets() -> None:
    settings = get_settings()
    store = S3ObjectStore(settings)
    retry("stockage S3", lambda: store.client.list_buckets())
    for bucket in settings.all_buckets:
        created = store.ensure_bucket(bucket)
        log.info("Bucket %s : %s", bucket, "créé" if created else "existant")


def bootstrap_catalog() -> None:
    """Crée les tables du catalogue JDBC, les namespaces et toutes les tables Iceberg (vides).

    Trino exige que les tables du catalogue JDBC existent déjà (seul Spark/Iceberg
    sait les créer) ; créer le modèle complet dès l'init rend aussi le schéma
    visible dans Trino/Superset avant le premier chargement.
    """
    from taxi_pipeline.spark import spark_session
    from taxi_pipeline.tables import ALL_TABLES, ensure_table

    settings = get_settings()
    with spark_session(settings, "bootstrap-catalog") as spark:
        for spec in ALL_TABLES:
            log.info("Table prête : %s", ensure_table(spark, settings, spec))


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    bootstrap_postgres()
    bootstrap_buckets()
    bootstrap_catalog()
    log.info("Plateforme initialisée")
    return 0


if __name__ == "__main__":
    sys.exit(main())
