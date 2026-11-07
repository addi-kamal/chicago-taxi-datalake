"""I/O de la couche gold : reconstruit les agrégats des mois touchés par la période."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_pipeline.config import Settings
from taxi_pipeline.gold import aggregates as agg
from taxi_pipeline.tables import (
    GOLD_COMPANY_PERF,
    GOLD_DAILY_REVENUE,
    GOLD_PAYMENT_MIX,
    GOLD_TOP_PICKUP,
    GOLD_TRIP_DURATION,
    SILVER_DIM_COMMUNITY_AREA,
    SILVER_TRIPS,
    TableSpec,
    ensure_table,
)

log = logging.getLogger(__name__)


def month_window(start: date, end: date) -> tuple[date, date]:
    """Mois complets couvrant ``[start, end)`` : ``[1er du mois de start, 1er du mois suivant)``.

    Les tables mensuelles (top zones, compagnies) sont ainsi toujours recalculées
    sur des mois entiers, jamais sur une fraction de mois.
    """
    first = start.replace(day=1)
    last_day = end - timedelta(days=1)
    nxt = (last_day.replace(day=1) + timedelta(days=32)).replace(day=1)
    return first, nxt


def build_gold(spark: SparkSession, settings: Settings, start: date, end: date) -> dict[str, int]:
    m_start, m_end = month_window(start, end)
    trips = (
        spark.table(settings.table("silver", SILVER_TRIPS.name))
        .where((F.col("trip_date") >= F.lit(m_start)) & (F.col("trip_date") < F.lit(m_end)))
        .cache()
    )
    dim = spark.table(settings.table("silver", SILVER_DIM_COMMUNITY_AREA.name))

    builders: list[tuple[TableSpec, Callable[[], DataFrame]]] = [
        (GOLD_DAILY_REVENUE, lambda: agg.daily_revenue(trips)),
        (GOLD_TRIP_DURATION, lambda: agg.trip_duration_stats(trips)),
        (GOLD_TOP_PICKUP, lambda: agg.top_pickup_areas(trips, dim)),
        (GOLD_PAYMENT_MIX, lambda: agg.payment_mix(trips)),
        (GOLD_COMPANY_PERF, lambda: agg.company_performance(trips)),
    ]
    counts: dict[str, int] = {}
    try:
        # Défense en profondeur (le gate qualité échoue déjà à 0 ligne) : un mois gold
        # n'est jamais écrasé par du vide.
        if trips.limit(1).count() == 0:
            raise ValueError(f"Silver vide sur {m_start} -> {m_end} : gold non modifié")
        for spec, build in builders:
            table = ensure_table(spark, settings, spec)
            # Tables gold petites : un fichier par partition mensuelle.
            df = build().coalesce(1)
            df.writeTo(table).overwrite(
                (F.col("trip_month") >= F.lit(m_start)) & (F.col("trip_month") < F.lit(m_end))
            )
            counts[spec.name] = (
                spark.table(table)
                .where(
                    (F.col("trip_month") >= F.lit(m_start)) & (F.col("trip_month") < F.lit(m_end))
                )
                .count()
            )
            log.info("Gold %s : %d lignes (%s -> %s)", table, counts[spec.name], m_start, m_end)
    finally:
        trips.unpersist()
    return counts
