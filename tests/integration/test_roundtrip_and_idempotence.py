"""Spark écrit / Trino lit, et idempotence du pipeline (2 exécutions = mêmes compteurs)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from taxi_pipeline.bronze.load import load_bronze
from taxi_pipeline.config import Settings
from taxi_pipeline.extract.landing import daterange, extract_days_from_sample
from taxi_pipeline.gold.build import build_gold
from taxi_pipeline.serving.trino_checks import query, validate_serving_layer
from taxi_pipeline.silver.load import load_community_areas, transform_silver
from taxi_pipeline.storage import S3ObjectStore
from taxi_pipeline.tables import GOLD_TABLES, ensure_namespace

pytestmark = pytest.mark.integration


def test_spark_writes_and_trino_reads_same_table(it_spark: Any, it_settings: Settings) -> None:
    ensure_namespace(it_spark, it_settings, "bronze")
    table = it_settings.table("bronze", "smoke_roundtrip")
    it_spark.sql(
        f"CREATE OR REPLACE TABLE {table} (id BIGINT, label STRING) USING iceberg "
        f"LOCATION '{it_settings.table_location('bronze', 'smoke_roundtrip')}'"
    )
    it_spark.range(1234).selectExpr("id", "concat('row-', id) AS label").writeTo(table).append()

    assert it_spark.table(table).count() == 1234
    assert query(it_settings, f"SELECT count(*), max(id) FROM {table}")[0] == [1234, 1233]


def _run_pipeline(spark: Any, settings: Settings, run_id: str) -> dict[str, int]:
    start, end = settings.pipeline_start_date, settings.pipeline_end_date
    days = daterange(start, end)
    store = S3ObjectStore(settings)
    for bucket in settings.all_buckets:
        store.ensure_bucket(bucket)
    extract_days_from_sample(store, settings, days, settings.sample_data_path, force=True)
    load_bronze(spark, settings, days, run_id)
    load_community_areas(spark, settings)
    silver = transform_silver(spark, settings, start, end, run_id)
    assert silver["gate_passed"], silver["gate_message"]
    build_gold(spark, settings, start, end)

    counts = {
        "bronze": spark.table(settings.table("bronze", "taxi_trips_raw")).count(),
        "silver": spark.table(settings.table("silver", "taxi_trips")).count(),
        "rejected": spark.table(settings.table("silver", "taxi_trips_rejected")).count(),
    }
    for spec in GOLD_TABLES:
        counts[spec.name] = spark.table(settings.table("gold", spec.name)).count()
    revenue = query(
        settings, f"SELECT sum(total_revenue) FROM {settings.table('gold', 'daily_revenue')}"
    )[0][0]
    counts["gold_revenue_cents"] = int(revenue * 100)
    return counts


def test_pipeline_is_idempotent_and_served_by_trino(it_spark: Any, it_settings: Settings) -> None:
    first = _run_pipeline(it_spark, it_settings, "it-run-1")
    second = _run_pipeline(it_spark, it_settings, "it-run-2")

    assert first == second
    assert first["bronze"] == 300
    assert first["silver"] == 289
    assert first["rejected"] == 8
    assert all(first[spec.name] > 0 for spec in GOLD_TABLES)

    checks = validate_serving_layer(
        it_settings, it_settings.pipeline_start_date, it_settings.pipeline_end_date, 289
    )
    assert all(c.passed for c in checks)


def test_dq_results_history_keeps_one_entry_per_rule_and_run(
    it_spark: Any, it_settings: Settings
) -> None:
    dq = it_settings.table("silver", "dq_results")
    rows = query(
        it_settings,
        f"SELECT run_id, count(*), count(DISTINCT rule) FROM {dq} GROUP BY run_id ORDER BY run_id",
    )
    assert [r[0] for r in rows] == ["it-run-1", "it-run-2"]
    assert all(r[1] == r[2] for r in rows)  # pas de doublon de règle pour un run


def test_timestamps_are_stored_without_timezone(it_spark: Any, it_settings: Settings) -> None:
    table = it_settings.table("silver", "taxi_trips")
    rows = query(
        it_settings,
        "SELECT typeof(trip_start_ts), CAST(trip_start_ts AS varchar) FROM "
        f"{table} WHERE trip_id = '78cb3aab64194c3aaf4dc97143e955b9cebe3d30'",
    )
    assert rows[0][0] == "timestamp(6)"  # sans fuseau côté Trino
    assert rows[0][1].startswith("2023-01-01 08:45:00")
    assert date(2023, 1, 1) <= it_settings.pipeline_start_date
