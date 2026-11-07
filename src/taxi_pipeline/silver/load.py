"""I/O de la couche silver : lecture bronze, écriture silver / quarantaine / DQ / référentiel."""

from __future__ import annotations

import csv
import logging
import os
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from taxi_pipeline.config import Settings
from taxi_pipeline.silver.quality import compute_dq_results, evaluate_gate
from taxi_pipeline.silver.transform import build_silver
from taxi_pipeline.tables import (
    BRONZE_TRIPS,
    SILVER_DIM_COMMUNITY_AREA,
    SILVER_DQ_RESULTS,
    SILVER_REJECTED,
    SILVER_TRIPS,
    ensure_table,
)

log = logging.getLogger(__name__)

DQ_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("period_start", DateType()),
        StructField("period_end", DateType()),
        StructField("rule", StringType()),
        StructField("severity", StringType()),
        StructField("checked_rows", LongType()),
        StructField("failed_rows", LongType()),
        StructField("failed_pct", DoubleType()),
        StructField("checked_at", TimestampType()),
    ]
)

DIM_SCHEMA = StructType(
    [
        StructField("community_area_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("side", StringType(), False),
    ]
)


def _sql_str(value: str) -> str:
    return value.replace("'", "''")


def load_community_areas(spark: SparkSession, settings: Settings) -> dict[str, int]:
    """Charge le référentiel versionné ``seeds/community_areas.csv`` (remplacement complet)."""
    path = os.path.join(settings.seeds_dir, "community_areas.csv")
    with open(path, encoding="utf-8", newline="") as fh:
        rows = [
            (int(r["community_area_id"]), r["name"].strip(), r["side"].strip())
            for r in csv.DictReader(fh)
        ]
    ids = {r[0] for r in rows}
    if ids != set(range(1, 78)):
        raise ValueError(
            f"Le référentiel doit contenir les zones 1..77 exactement ({len(ids)} lues)"
        )
    table = ensure_table(spark, settings, SILVER_DIM_COMMUNITY_AREA)
    spark.createDataFrame(rows, DIM_SCHEMA).coalesce(1).writeTo(table).overwrite(F.lit(True))
    log.info("Référentiel %s : %d zones", table, len(rows))
    return {"rows": len(rows)}


def transform_silver(
    spark: SparkSession, settings: Settings, start: date, end: date, run_id: str
) -> dict[str, Any]:
    """Bronze [start, end) -> silver + quarantaine + résultats qualité."""
    bronze_table = settings.table("bronze", BRONZE_TRIPS.name)
    last_day = end - timedelta(days=1)
    raw = spark.table(bronze_table).where(F.col("trip_date").between(F.lit(start), F.lit(last_day)))
    input_rows = raw.count()

    frames = build_silver(raw, start, end)
    annotated = frames.annotated.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        deduped_rows = annotated.count()
        valid = frames.valid
        rejected = frames.rejected

        silver_table = ensure_table(spark, settings, SILVER_TRIPS)
        valid.writeTo(silver_table).overwritePartitions()

        rejected_table = ensure_table(spark, settings, SILVER_REJECTED)
        # overwrite(filtre) : remplace aussi les jours qui n'ont plus aucun rejet.
        rejected.writeTo(rejected_table).overwrite(
            F.col("trip_date").between(F.lit(start), F.lit(last_day))
        )

        dq = compute_dq_results(
            annotated,
            spark.table(silver_table).where(
                F.col("trip_date").between(F.lit(start), F.lit(last_day))
            ),
            input_rows=input_rows,
            duplicates_removed=input_rows - deduped_rows,
            run_id=run_id,
            period_start=start,
            period_end=end,
            checked_at=datetime.now(UTC).replace(tzinfo=None),
        )
    finally:
        annotated.unpersist()

    dq_table = ensure_table(spark, settings, SILVER_DQ_RESULTS)
    spark.sql(f"DELETE FROM {dq_table} WHERE run_id = '{_sql_str(run_id)}'")
    spark.createDataFrame(dq, DQ_SCHEMA).coalesce(1).writeTo(dq_table).append()

    gate = evaluate_gate(dq, settings.dq_max_reject_pct)
    valid_rows = next(r["checked_rows"] for r in dq if r["rule"] == "is_duration_outlier")
    summary: dict[str, Any] = {
        "input_rows": input_rows,
        "duplicates_removed": input_rows - deduped_rows,
        "valid_rows": int(valid_rows),
        "rejected_rows": gate.rejected_rows,
        "reject_pct": gate.reject_pct,
        "gate_passed": gate.passed,
        "gate_message": gate.message(),
        "dq_results": [
            {k: (v.isoformat() if isinstance(v, date | datetime) else v) for k, v in r.items()}
            for r in dq
        ],
    }
    log.info(
        "Silver : %d entrées, %d doublons, %d valides, %d rejetées",
        input_rows,
        summary["duplicates_removed"],
        summary["valid_rows"],
        summary["rejected_rows"],
    )
    return summary
