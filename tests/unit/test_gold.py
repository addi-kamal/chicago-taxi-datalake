"""Agrégats gold : cohérence avec silver, parts, rangs, exclusion des outliers."""

from __future__ import annotations

import csv
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.gold import aggregates as agg
from taxi_pipeline.silver.load import DIM_SCHEMA
from taxi_pipeline.silver.transform import build_silver
from taxi_pipeline.tables import GOLD_TABLES
from tests.conftest import ROOT, SAMPLE_END, SAMPLE_START

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def trips(bronze_sample):  # type: ignore[no-untyped-def]
    return build_silver(bronze_sample, SAMPLE_START, SAMPLE_END).valid.cache()


@pytest.fixture(scope="module")
def dim(spark):  # type: ignore[no-untyped-def]
    with open(ROOT / "seeds" / "community_areas.csv", encoding="utf-8") as fh:
        rows = [(int(r["community_area_id"]), r["name"], r["side"]) for r in csv.DictReader(fh)]
    return spark.createDataFrame(rows, DIM_SCHEMA)


def _columns(name: str) -> list[str]:
    spec = next(s for s in GOLD_TABLES if s.name == name)
    return [line.split()[0] for line in spec.columns.strip().split(",\n")]


def test_outputs_match_gold_ddl(trips, dim) -> None:  # type: ignore[no-untyped-def]
    outputs = {
        "daily_revenue": agg.daily_revenue(trips),
        "trip_duration_stats": agg.trip_duration_stats(trips),
        "top_pickup_areas": agg.top_pickup_areas(trips, dim),
        "payment_mix": agg.payment_mix(trips),
        "company_performance": agg.company_performance(trips),
    }
    for name, df in outputs.items():
        assert df.columns == _columns(name), name


def test_daily_revenue_reconciles_with_silver(trips) -> None:  # type: ignore[no-untyped-def]
    daily = agg.daily_revenue(trips)
    assert daily.count() == 3
    total_gold = daily.agg(F.sum("total_revenue")).first()[0]
    total_silver = trips.agg(F.sum("trip_total")).first()[0]
    assert Decimal(total_gold) == Decimal(total_silver)
    assert daily.agg(F.sum("trips")).first()[0] == trips.count()


def test_payment_shares_sum_to_one_per_day(trips) -> None:  # type: ignore[no-untyped-def]
    mix = agg.payment_mix(trips)
    for row in mix.groupBy("trip_date").agg(F.sum("share_of_trips").alias("s")).collect():
        assert row["s"] == pytest.approx(1.0, abs=1e-3)
    assert mix.where(F.col("payment_type") == "Unknown").count() >= 1  # payment_type vide


def test_top_pickup_areas_ranked_and_named(trips, dim) -> None:  # type: ignore[no-untyped-def]
    top = agg.top_pickup_areas(trips, dim)
    first = top.where(F.col("area_rank") == 1).first()
    assert first["community_area_id"] == 8 and first["community_area_name"] == "Near North Side"
    assert top.select("area_rank").distinct().count() == top.count()  # un seul mois
    assert top.where(F.col("community_area_name") == "Unknown").count() == 0


def test_duration_stats_exclude_outliers(trips) -> None:  # type: ignore[no-untyped-def]
    stats = agg.trip_duration_stats(trips)
    clean = trips.where(~F.col("is_duration_outlier") & ~F.col("is_distance_outlier")).count()
    assert stats.agg(F.sum("trips")).first()[0] == clean
    row = stats.where(F.col("trip_date") == "2023-01-01").first()
    assert row["day_of_week"] == 7 and row["day_label"] == "7-Dim"  # 01/01/2023 = dimanche
    assert stats.where(F.col("median_duration_min") > F.col("p90_duration_min")).count() == 0


def test_company_ranking(trips) -> None:  # type: ignore[no-untyped-def]
    perf = agg.company_performance(trips).orderBy("revenue_rank").collect()
    revenues = [r["total_revenue"] for r in perf]
    assert revenues == sorted(revenues, reverse=True)
    assert {r["company"] for r in perf} >= {"Flash Cab", "Sun Taxi"}
