"""Agrégats métier silver -> gold (fonctions pures ``DataFrame -> DataFrame``).

Définitions des KPIs : README, section « Modèle de données ».

Conventions :
- les KPIs financiers portent sur **tous** les trajets valides (le CA est le CA) ;
- les KPIs de durée / distance / vitesse excluent les outliers signalés ;
- ``tip_rate`` = somme des pourboires / somme des tarifs (``fare``). Les pourboires
  en espèces ne sont pas déclarés dans la source : le taux est donc sous-estimé
  pour les paiements cash (cf. ``payment_mix``).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

MONEY = "decimal(18,2)"
TICKET = "decimal(10,2)"
UNKNOWN = "Unknown"


def _month(col: str = "trip_date") -> Column:
    return F.trunc(F.col(col), "month").alias("trip_month")


def _ratio(num: Column, den: Column, digits: int = 4) -> Column:
    return F.round(F.when(den != 0, num.cast("double") / den.cast("double")), digits)


def _money(col: Column) -> Column:
    return col.cast(MONEY)


def daily_revenue(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("trip_date", _month())
        .agg(
            F.count(F.lit(1)).alias("trips"),
            _money(F.sum("trip_total")).alias("total_revenue"),
            _money(F.sum("fare")).alias("fare_revenue"),
            _money(F.sum("tips")).alias("tips_revenue"),
            _money(F.sum("tolls")).alias("tolls_revenue"),
            _money(F.sum("extras")).alias("extras_revenue"),
            F.avg("trip_total").cast(TICKET).alias("avg_ticket"),
            _ratio(F.sum("tips"), F.sum("fare")).alias("tip_rate"),
            F.countDistinct("taxi_id").cast("bigint").alias("active_taxis"),
        )
        .select(
            "trip_date",
            "trip_month",
            "trips",
            "total_revenue",
            "fare_revenue",
            "tips_revenue",
            "tolls_revenue",
            "extras_revenue",
            "avg_ticket",
            "tip_rate",
            "active_taxis",
        )
    )


DAY_LABELS = ("1-Lun", "2-Mar", "3-Mer", "4-Jeu", "5-Ven", "6-Sam", "7-Dim")


def _day_label(iso_day_of_week: Column) -> Column:
    """1 -> '1-Lun' ... 7 -> '7-Dim' (préfixe numérique : tri naturel dans la BI)."""
    labels = F.create_map(
        *[x for i, lbl in enumerate(DAY_LABELS, 1) for x in (F.lit(i), F.lit(lbl))]
    )
    return labels[iso_day_of_week]


def trip_duration_stats(trips: DataFrame) -> DataFrame:
    clean = trips.where(~F.col("is_duration_outlier") & ~F.col("is_distance_outlier"))
    # dayofweek() : 1 = dimanche ... 7 = samedi -> ISO : 1 = lundi ... 7 = dimanche
    iso_dow = ((F.dayofweek("trip_date") + 5) % 7) + 1
    minutes = F.col("trip_seconds") / F.lit(60.0)
    return (
        clean.withColumn("trip_hour", F.hour("trip_start_ts"))
        .withColumn("day_of_week", iso_dow)
        .groupBy("trip_date", _month(), "trip_hour", "day_of_week")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg(minutes), 2).alias("avg_duration_min"),
            F.round(F.percentile_approx("trip_seconds", 0.5) / 60.0, 2).alias(
                "median_duration_min"
            ),
            F.round(F.percentile_approx("trip_seconds", 0.9) / 60.0, 2).alias("p90_duration_min"),
            F.round(F.avg("trip_miles"), 2).alias("avg_distance_miles"),
            F.round(F.sum("trip_miles") / (F.sum("trip_seconds") / F.lit(3600.0)), 2).alias(
                "avg_speed_mph"
            ),
        )
        .select(
            "trip_date",
            "trip_month",
            F.col("trip_hour").cast("int"),
            F.col("day_of_week").cast("int"),
            _day_label(F.col("day_of_week")).alias("day_label"),
            "trips",
            F.col("avg_duration_min").cast("double"),
            F.col("median_duration_min").cast("double"),
            F.col("p90_duration_min").cast("double"),
            F.col("avg_distance_miles").cast("double"),
            F.col("avg_speed_mph").cast("double"),
        )
    )


def top_pickup_areas(trips: DataFrame, dim_community_area: DataFrame) -> DataFrame:
    per_area = (
        trips.where(F.col("pickup_community_area").isNotNull())
        .groupBy(_month(), F.col("pickup_community_area").alias("community_area_id"))
        .agg(
            F.count(F.lit(1)).alias("trips"),
            _money(F.sum("trip_total")).alias("total_revenue"),
            F.avg("trip_total").cast(TICKET).alias("avg_ticket"),
        )
    )
    month_w = Window.partitionBy("trip_month")
    rank_w = Window.partitionBy("trip_month").orderBy(F.desc("trips"), F.asc("community_area_id"))
    dim = dim_community_area.select(
        "community_area_id", F.col("name").alias("community_area_name"), "side"
    )
    return (
        per_area.join(F.broadcast(dim), "community_area_id", "left")
        .withColumn("share_of_trips", _ratio(F.col("trips"), F.sum("trips").over(month_w)))
        .withColumn("area_rank", F.row_number().over(rank_w))
        .select(
            "trip_month",
            F.col("community_area_id").cast("int"),
            F.coalesce("community_area_name", F.lit(UNKNOWN)).alias("community_area_name"),
            F.coalesce("side", F.lit(UNKNOWN)).alias("side"),
            "trips",
            "total_revenue",
            "avg_ticket",
            "share_of_trips",
            F.col("area_rank").cast("int"),
        )
    )


def payment_mix(trips: DataFrame) -> DataFrame:
    per_type = (
        trips.withColumn("payment_type", F.coalesce("payment_type", F.lit(UNKNOWN)))
        .groupBy("trip_date", _month(), "payment_type")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            _money(F.sum("trip_total")).alias("total_revenue"),
        )
    )
    day_w = Window.partitionBy("trip_date")
    return per_type.select(
        "trip_date",
        "trip_month",
        "payment_type",
        "trips",
        "total_revenue",
        _ratio(F.col("trips"), F.sum("trips").over(day_w)).alias("share_of_trips"),
        _ratio(F.col("total_revenue"), F.sum("total_revenue").over(day_w)).alias(
            "share_of_revenue"
        ),
    )


def company_performance(trips: DataFrame) -> DataFrame:
    distance_ok = ~F.col("is_distance_outlier")
    per_company = (
        trips.withColumn("company", F.coalesce("company", F.lit(UNKNOWN)))
        .groupBy(_month(), "company")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.countDistinct("taxi_id").cast("bigint").alias("active_taxis"),
            _money(F.sum("trip_total")).alias("total_revenue"),
            F.avg("trip_total").cast(TICKET).alias("avg_ticket"),
            _ratio(F.sum("tips"), F.sum("fare")).alias("tip_rate"),
            F.round(F.avg(F.when(distance_ok, F.col("trip_miles"))), 2).alias("avg_trip_miles"),
        )
    )
    rank_w = Window.partitionBy("trip_month").orderBy(F.desc("total_revenue"), F.asc("company"))
    return per_company.select(
        "trip_month",
        "company",
        "trips",
        "active_taxis",
        "total_revenue",
        "avg_ticket",
        "tip_rate",
        F.col("avg_trip_miles").cast("double"),
        F.row_number().over(rank_w).cast("int").alias("revenue_rank"),
    )
