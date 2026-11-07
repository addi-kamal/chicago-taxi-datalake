"""Fixtures partagées : SparkSession locale (sans S3 ni catalogue) et données d'exemple."""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CSV = ROOT / "tests" / "fixtures" / "sample_trips.csv"
SAMPLE_START = date(2023, 1, 1)
SAMPLE_END = date(2023, 1, 4)


def _java_available() -> bool:
    return bool(os.environ.get("JAVA_HOME")) or shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    if not _java_available():
        pytest.skip("Java introuvable : tests Spark ignorés")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-pipeline-unit-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_sample(spark):  # type: ignore[no-untyped-def]
    """Le CSV d'exemple au format de la table bronze (colonnes techniques incluses)."""
    from pyspark.sql import functions as F

    from taxi_pipeline.bronze.load import add_ingestion_metadata, read_landing

    df = add_ingestion_metadata(read_landing(spark, [str(SAMPLE_CSV)]), run_id="test-run")
    # Hors S3, le chemin ne contient pas `trip_date=` : on dérive la partition du timestamp.
    return df.withColumn(
        "trip_date",
        F.coalesce(F.to_date(F.substring("trip_start_timestamp", 1, 10)), F.lit(SAMPLE_START)),
    ).cache()
