"""Chargement landing (CSV bruts) -> ``iceberg.bronze.taxi_trips_raw``.

- schéma explicite tout-string, en-tête CSV vérifié (``enforceSchema=false``) ;
- aucune modification de valeur : seules des colonnes techniques sont ajoutées ;
- idempotent : ``overwritePartitions()`` remplace exactement les jours chargés.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.config import Settings
from taxi_pipeline.extract.landing import landing_uri
from taxi_pipeline.schemas import LANDING_SCHEMA, SOURCE_COLUMNS
from taxi_pipeline.tables import BRONZE_TRIPS, ensure_table

log = logging.getLogger(__name__)

_TRIP_DATE_FROM_PATH = r"trip_date=(\d{4}-\d{2}-\d{2})"


def read_landing(spark: SparkSession, paths: Sequence[str]) -> DataFrame:
    return (
        spark.read.format("csv")
        .schema(LANDING_SCHEMA)
        .option("header", "true")
        .option("enforceSchema", "false")  # échoue si l'en-tête ne colle pas au contrat
        .option("mode", "FAILFAST")
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .load(list(paths))
    )


def add_ingestion_metadata(df: DataFrame, run_id: str) -> DataFrame:
    """Fonction pure : ajoute les colonnes techniques bronze."""
    source_file = F.col("_metadata.file_path")
    return df.select(
        *[F.col(c) for c in SOURCE_COLUMNS],
        source_file.alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(run_id).alias("_run_id"),
        F.to_date(F.regexp_extract(source_file, _TRIP_DATE_FROM_PATH, 1)).alias("trip_date"),
    )


def load_bronze(
    spark: SparkSession, settings: Settings, days: Sequence[date], run_id: str
) -> dict[str, int]:
    table = ensure_table(spark, settings, BRONZE_TRIPS)
    if not days:
        log.warning("Aucun jour à charger en bronze")
        return {"rows": 0, "days": 0}

    paths = [landing_uri(settings, d) for d in days]
    df = add_ingestion_metadata(read_landing(spark, paths), run_id)
    # write.distribution-mode=hash : Iceberg regroupe les lignes par partition avant
    # écriture -> un fichier par jour au lieu d'un fichier par tâche Spark.
    df.writeTo(table).overwritePartitions()

    rows = (
        spark.table(table)
        .where(F.col("trip_date").between(F.lit(min(days)), F.lit(max(days))))
        .count()
    )
    log.info("Bronze %s : %d lignes pour %d jours", table, rows, len(days))
    return {"rows": rows, "days": len(days)}
