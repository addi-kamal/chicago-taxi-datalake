"""Transformations silver et règles qualité sur le jeu d'exemple (cas sales inclus).

Volumes attendus (cf. scripts/generate_sample_fixture.py) :
300 lignes = 280 valides + 3 doublons + 8 rejets bloquants + 9 lignes nettoyées/signalées.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from taxi_pipeline.schemas import REJECTED_COLUMNS, SILVER_COLUMNS
from taxi_pipeline.silver.quality import compute_dq_results, evaluate_gate
from taxi_pipeline.silver.transform import build_silver
from tests.conftest import SAMPLE_END, SAMPLE_START

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def frames(bronze_sample):  # type: ignore[no-untyped-def]
    return build_silver(bronze_sample, SAMPLE_START, SAMPLE_END)


def test_output_schemas_match_table_contracts(frames) -> None:  # type: ignore[no-untyped-def]
    assert tuple(frames.valid.columns) == SILVER_COLUMNS
    assert tuple(frames.rejected.columns) == REJECTED_COLUMNS
    types = dict(frames.valid.dtypes)
    assert types["trip_start_ts"] == "timestamp_ntz"
    assert types["fare"] == "decimal(10,2)"
    assert types["trip_seconds"] == "int"
    assert types["pickup_community_area"] == "int"


def test_volumes_dedup_and_quarantine(frames, bronze_sample) -> None:  # type: ignore[no-untyped-def]
    assert bronze_sample.count() == 300
    assert frames.annotated.count() == 297  # 3 doublons supprimés
    assert frames.rejected.count() == 8
    assert frames.valid.count() == 289
    assert frames.valid.select("trip_id").distinct().count() == 289


def test_reject_reasons(frames) -> None:  # type: ignore[no-untyped-def]
    reasons = {
        r["reject_reason"]: r["count"]
        for r in frames.rejected.groupBy("reject_reason").count().collect()
    }
    assert reasons == {
        "missing_trip_id": 2,
        "end_before_start": 2,
        "negative_amount": 2,
        "missing_or_invalid_start_ts": 1,
        "start_ts_out_of_period": 1,
    }


def test_rejected_rows_keep_raw_values(frames) -> None:  # type: ignore[no-untyped-def]
    bad_ts = frames.rejected.where(F.col("reject_reason") == "missing_or_invalid_start_ts").first()
    assert bad_ts["trip_start_timestamp"] == "not-a-date"


def test_timestamps_are_local_wall_clock_without_timezone_shift(frames) -> None:  # type: ignore[no-untyped-def]
    row = frames.valid.where(F.col("trip_id") == "78cb3aab64194c3aaf4dc97143e955b9cebe3d30").first()
    assert row["trip_start_ts"] == datetime(2023, 1, 1, 8, 45)
    assert row["trip_date"].isoformat() == "2023-01-01"
    assert row["fare"] == Decimal("16.54")


def test_cleaning_rules(frames) -> None:  # type: ignore[no-untyped-def]
    v = frames.valid
    assert v.where(~F.col("pickup_community_area").between(1, 77)).count() == 0
    assert v.where(~F.col("dropoff_community_area").between(1, 77)).count() == 0
    assert v.where(F.col("pickup_lat") < 41.6).count() == 0
    # chaîne vide -> null ; espaces supprimés ; symbole $ toléré
    assert v.where(F.col("company") == "  Flash Cab  ").count() == 0
    assert v.where(F.col("payment_type").isNull()).count() == 1
    assert v.where(F.col("fare").isNull()).count() == 0


def test_quality_flags(frames) -> None:  # type: ignore[no-untyped-def]
    v = frames.valid
    assert v.where("is_duration_outlier").count() == 2
    assert v.where("is_distance_outlier").count() == 1
    assert v.where("is_amount_inconsistent").count() == 1


def test_dq_results_and_gate(frames, bronze_sample) -> None:  # type: ignore[no-untyped-def]
    results = compute_dq_results(
        frames.annotated,
        frames.valid,
        input_rows=bronze_sample.count(),
        duplicates_removed=3,
        run_id="test-run",
        period_start=SAMPLE_START,
        period_end=SAMPLE_END,
        checked_at=datetime(2023, 1, 4),
    )
    by_rule = {r["rule"]: r for r in results}
    assert by_rule["duplicate_trip_id"]["failed_rows"] == 3
    assert by_rule["total_rejected"]["failed_rows"] == 8
    assert by_rule["total_rejected"]["checked_rows"] == 297
    assert by_rule["community_area_nullified"]["failed_rows"] == 2
    assert by_rule["coordinates_nullified"]["failed_rows"] == 1
    assert by_rule["is_duration_outlier"]["checked_rows"] == 289

    assert evaluate_gate(results, max_reject_pct=5).passed
    gate = evaluate_gate(results, max_reject_pct=1)
    assert not gate.passed and "ÉCHEC" in gate.message()


def test_transformation_is_deterministic(bronze_sample) -> None:  # type: ignore[no-untyped-def]
    a = build_silver(bronze_sample, SAMPLE_START, SAMPLE_END).valid
    b = build_silver(bronze_sample, SAMPLE_START, SAMPLE_END).valid
    assert a.exceptAll(b).count() == 0 and b.exceptAll(a).count() == 0
