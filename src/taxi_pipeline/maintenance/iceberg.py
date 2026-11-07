"""Maintenance des tables Iceberg (portée par Spark uniquement).

- ``rewrite_data_files`` : compaction des petits fichiers ;
- ``rewrite_manifests``  : regroupement des manifests ;
- ``expire_snapshots``   : rétention des snapshots (time travel borné) ;
- ``remove_orphan_files``: nettoyage des fichiers non référencés (> 3 jours).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession

from taxi_pipeline.config import Settings
from taxi_pipeline.tables import ALL_TABLES

log = logging.getLogger(__name__)

ORPHAN_SAFETY_DAYS = 3  # ne jamais supprimer un fichier potentiellement en cours d'écriture


def _table_exists(spark: SparkSession, identifier: str) -> bool:
    return bool(spark.catalog.tableExists(identifier))


def maintain_tables(
    spark: SparkSession, settings: Settings, snapshot_retention_days: int = 7
) -> dict[str, Any]:
    catalog = settings.iceberg_catalog_name
    now = datetime.now(UTC).replace(tzinfo=None)
    expire_before = (now - timedelta(days=snapshot_retention_days)).strftime("%Y-%m-%d %H:%M:%S")
    orphan_before = (now - timedelta(days=ORPHAN_SAFETY_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

    report: dict[str, Any] = {}
    errors: list[str] = []
    for spec in ALL_TABLES:
        full = settings.table(spec.layer, spec.name)
        short = f"{settings.namespace(spec.layer)}.{spec.name}"
        if not _table_exists(spark, full):
            log.info("Table absente, maintenance ignorée : %s", full)
            continue
        procedures = {
            "rewrite_data_files": (
                f"CALL {catalog}.system.rewrite_data_files("
                f"table => '{short}', options => map('min-input-files', '2'))"
            ),
            "rewrite_manifests": f"CALL {catalog}.system.rewrite_manifests(table => '{short}')",
            "expire_snapshots": (
                f"CALL {catalog}.system.expire_snapshots(table => '{short}', "
                f"older_than => TIMESTAMP '{expire_before}', retain_last => 5)"
            ),
            "remove_orphan_files": (
                f"CALL {catalog}.system.remove_orphan_files(table => '{short}', "
                f"older_than => TIMESTAMP '{orphan_before}')"
            ),
        }
        table_report: dict[str, Any] = {}
        for name, sql in procedures.items():
            try:
                rows = spark.sql(sql).collect()
                table_report[name] = [r.asDict() for r in rows][:1]
            except Exception as exc:
                log.exception("Maintenance %s sur %s en échec", name, full)
                errors.append(f"{full}:{name}: {exc}")
                table_report[name] = "error"
        report[full] = table_report
        log.info("Maintenance %s : %s", full, table_report)

    if errors:
        raise RuntimeError(f"{len(errors)} opération(s) de maintenance en échec : {errors}")
    return report
