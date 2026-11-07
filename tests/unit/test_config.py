from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from taxi_pipeline.config import Settings
from taxi_pipeline.gold.build import month_window
from taxi_pipeline.spark import spark_conf
from taxi_pipeline.tables import ALL_TABLES, SILVER_TRIPS, create_table_ddl


def test_same_identifier_for_spark_and_trino() -> None:
    s = Settings()
    assert s.table("gold", "daily_revenue") == "iceberg.gold.daily_revenue"
    assert s.table_location("silver", "taxi_trips") == "s3://silver/warehouse/silver/taxi_trips"


def test_namespace_prefix_isolates_test_tables() -> None:
    s = Settings(iceberg_namespace_prefix="it_")
    assert s.table("bronze", "taxi_trips_raw") == "iceberg.it_bronze.taxi_trips_raw"


def test_invalid_period_rejected() -> None:
    with pytest.raises(ValidationError):
        Settings(pipeline_start_date=date(2023, 2, 1), pipeline_end_date=date(2023, 1, 1))


def test_spark_conf_is_storage_agnostic_and_path_style() -> None:
    s = Settings(s3_endpoint="https://rgw.ceph.internal", s3_use_ssl=True)
    conf = spark_conf(s)
    assert conf["spark.sql.catalog.iceberg.s3.endpoint"] == "https://rgw.ceph.internal"
    assert conf["spark.sql.catalog.iceberg.s3.path-style-access"] == "true"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "true"
    assert conf["spark.sql.catalog.iceberg.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf["spark.sql.defaultCatalog"] == "iceberg"


def test_ddl_contains_partitioning_location_and_format() -> None:
    ddl = create_table_ddl(Settings(), SILVER_TRIPS)
    assert "PARTITIONED BY (days(trip_start_ts))" in ddl
    assert "LOCATION 's3://silver/warehouse/silver/taxi_trips'" in ddl
    assert "'format-version' = '2'" in ddl
    assert "TIMESTAMP_NTZ" in ddl


def test_every_table_lives_in_its_layer_bucket() -> None:
    s = Settings()
    for spec in ALL_TABLES:
        assert s.table_location(spec.layer, spec.name).startswith(f"s3://{spec.layer}/")


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        (date(2023, 1, 1), date(2023, 4, 1), (date(2023, 1, 1), date(2023, 4, 1))),
        (date(2023, 1, 15), date(2023, 1, 16), (date(2023, 1, 1), date(2023, 2, 1))),
        (date(2023, 12, 20), date(2024, 1, 1), (date(2023, 12, 1), date(2024, 1, 1))),
    ],
)
def test_gold_month_window_always_covers_full_months(
    start: date, end: date, expected: tuple[date, date]
) -> None:
    assert month_window(start, end) == expected
