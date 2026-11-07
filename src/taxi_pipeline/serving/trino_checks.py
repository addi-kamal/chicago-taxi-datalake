"""Validation de la couche d'exposition : Trino relit ce que Spark a écrit.

Ces contrôles prouvent le principe « Spark écrit, Trino lit » sur le même
catalogue Iceberg et le même stockage S3, et que chaque table gold est servie.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import trino

from taxi_pipeline.config import Settings
from taxi_pipeline.tables import GOLD_TABLES, SILVER_TRIPS

log = logging.getLogger(__name__)


class ServingCheckError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    value: int
    expected: str
    passed: bool


def connect(settings: Settings) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user=settings.trino_user,
        catalog=settings.iceberg_catalog_name,
        http_scheme="http",
    )


def query(settings: Settings, sql: str) -> list[list[Any]]:
    conn = connect(settings)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows: list[list[Any]] = cur.fetchall()
        return rows
    finally:
        conn.close()


def wait_for_trino(settings: Settings, timeout_s: int = 300) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            query(settings, "SELECT 1")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(5)
    raise ServingCheckError(f"Trino injoignable après {timeout_s}s : {last_error}")


def validate_serving_layer(
    settings: Settings, start: date, end: date, expected_silver_rows: int | None = None
) -> list[CheckResult]:
    """Échoue si une table gold est vide, si Trino ne relit pas le volume silver attendu,
    ou si gold.daily_revenue ne réconcilie pas avec silver (trajets et CA)."""
    wait_for_trino(settings)
    last_day = end - timedelta(days=1)
    results: list[CheckResult] = []

    silver = settings.table("silver", SILVER_TRIPS.name)
    silver_rows = int(
        query(
            settings,
            f"SELECT count(*) FROM {silver} "
            f"WHERE trip_date BETWEEN DATE '{start}' AND DATE '{last_day}'",
        )[0][0]
    )
    if expected_silver_rows is None:
        results.append(CheckResult("silver.taxi_trips", silver_rows, "> 0", silver_rows > 0))
    else:
        results.append(
            CheckResult(
                "silver.taxi_trips (Spark == Trino)",
                silver_rows,
                f"== {expected_silver_rows}",
                silver_rows == expected_silver_rows,
            )
        )

    # Réconciliation gold / silver sur le même périmètre de jours : trajets et CA exacts
    # (décimales des deux côtés, comparées en centimes).
    daily = settings.table("gold", "daily_revenue")
    period = f"BETWEEN DATE '{start}' AND DATE '{last_day}'"
    gold_trips, gold_cents = query(
        settings,
        "SELECT coalesce(sum(trips), 0), "
        "CAST(round(coalesce(sum(total_revenue), 0) * 100) AS bigint) "
        f"FROM {daily} WHERE trip_date {period}",
    )[0]
    silver_cents = int(
        query(
            settings,
            "SELECT CAST(round(coalesce(sum(trip_total), 0) * 100) AS bigint) "
            f"FROM {silver} WHERE trip_date {period}",
        )[0][0]
    )
    results.append(
        CheckResult(
            "gold.daily_revenue trips == silver",
            int(gold_trips),
            f"== {silver_rows}",
            int(gold_trips) == silver_rows,
        )
    )
    results.append(
        CheckResult(
            "gold.daily_revenue revenue (cents) == silver",
            int(gold_cents),
            f"== {silver_cents}",
            int(gold_cents) == silver_cents,
        )
    )

    for spec in GOLD_TABLES:
        table = settings.table("gold", spec.name)
        n = int(
            query(
                settings,
                f"SELECT count(*) FROM {table} "
                f"WHERE trip_month BETWEEN date_trunc('month', DATE '{start}') "
                f"AND DATE '{last_day}'",
            )[0][0]
        )
        results.append(CheckResult(f"gold.{spec.name}", n, "> 0", n > 0))

    for r in results:
        log.info(
            "[%s] %s = %d (attendu %s)", "OK" if r.passed else "KO", r.name, r.value, r.expected
        )
    failed = [r for r in results if not r.passed]
    if failed:
        raise ServingCheckError(f"Contrôles Trino en échec : {[f.name for f in failed]}")
    return results
