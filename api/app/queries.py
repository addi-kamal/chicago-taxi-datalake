"""Toutes les requêtes SQL de l'API, au même endroit.

Règles suivies :
- on ne lit que la couche **gold** (et ``silver.dq_results`` pour la qualité) ;
- les montants ``DECIMAL`` sont convertis en ``double`` (JSON numérique, pas de chaîne) ;
- quand on agrège plusieurs jours, on **recalcule** les ratios à partir des sommes
  (CA / trajets, pourboires / tarifs). On ne fait jamais la moyenne de moyennes
  quotidiennes, qui donnerait un résultat faux ;
- les médianes et percentiles ne s'additionnent pas : ils ne sont donc exposés que
  jour par jour (``/v1/durations/daily``), jamais agrégés sur une période.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Literal

Query = tuple[str, list[Any]]

CompanySort = Literal["revenue", "trips", "avg_ticket", "tip_rate"]

# Liste blanche : la colonne de tri vient de ce dictionnaire, jamais de l'utilisateur.
_COMPANY_ORDER: dict[str, str] = {
    "revenue": "total_revenue DESC",
    "trips": "trips DESC",
    "avg_ticket": "avg_ticket DESC",
    "tip_rate": "tip_rate DESC",
}

HEALTH = "SELECT 1"


def metadata() -> Query:
    return (
        """
        SELECT min(trip_date) AS first_day,
               max(trip_date) AS last_day,
               count(*) AS days,
               coalesce(sum(trips), 0) AS trips,
               (SELECT max(committed_at) FROM gold."daily_revenue$snapshots") AS data_updated_at
        FROM gold.daily_revenue
        """,
        [],
    )


def available_months() -> Query:
    return (
        "SELECT DISTINCT trip_month FROM gold.daily_revenue ORDER BY trip_month",
        [],
    )


def kpis(start: date, end: date) -> Query:
    return (
        """
        SELECT min(trip_date) AS first_day,
               max(trip_date) AS last_day,
               count(*) AS days,
               coalesce(sum(trips), 0) AS trips,
               CAST(round(coalesce(sum(total_revenue), 0), 2) AS double) AS total_revenue,
               CAST(round(sum(total_revenue) / nullif(sum(trips), 0), 2) AS double) AS avg_ticket,
               round(CAST(sum(tips_revenue) AS double)
                     / nullif(CAST(sum(fare_revenue) AS double), 0), 4) AS tip_rate
        FROM gold.daily_revenue
        WHERE trip_date >= ? AND trip_date < ?
        """,
        [start, end],
    )


def daily_revenue(start: date, end: date) -> Query:
    return (
        """
        SELECT trip_date,
               trips,
               CAST(total_revenue AS double) AS total_revenue,
               CAST(fare_revenue AS double) AS fare_revenue,
               CAST(tips_revenue AS double) AS tips_revenue,
               CAST(tolls_revenue AS double) AS tolls_revenue,
               CAST(extras_revenue AS double) AS extras_revenue,
               CAST(avg_ticket AS double) AS avg_ticket,
               round(tip_rate, 4) AS tip_rate,
               active_taxis
        FROM gold.daily_revenue
        WHERE trip_date >= ? AND trip_date < ?
        ORDER BY trip_date
        """,
        [start, end],
    )


def monthly_revenue(start: date, end: date) -> Query:
    return (
        """
        SELECT trip_month,
               count(*) AS days,
               sum(trips) AS trips,
               CAST(sum(total_revenue) AS double) AS total_revenue,
               CAST(round(sum(total_revenue) / nullif(sum(trips), 0), 2) AS double) AS avg_ticket,
               round(CAST(sum(tips_revenue) AS double)
                     / nullif(CAST(sum(fare_revenue) AS double), 0), 4) AS tip_rate
        FROM gold.daily_revenue
        WHERE trip_date >= ? AND trip_date < ?
        GROUP BY trip_month
        ORDER BY trip_month
        """,
        [start, end],
    )


def durations_by_hour(start: date, end: date, day_of_week: int | None) -> Query:
    # Moyennes pondérées par le nombre de trajets : une heure à 10 trajets ne doit
    # pas peser autant qu'une heure à 10 000.
    dow_filter, params = ("", [start, end])
    if day_of_week is not None:
        dow_filter, params = ("AND day_of_week = ?", [start, end, day_of_week])
    return (
        f"""
        SELECT trip_hour AS hour,
               sum(trips) AS trips,
               round(sum(avg_duration_min * trips) / nullif(sum(trips), 0), 2) AS avg_duration_min,
               round(sum(avg_distance_miles * trips) / nullif(sum(trips), 0), 2)
                   AS avg_distance_miles,
               round(sum(avg_speed_mph * trips) / nullif(sum(trips), 0), 2) AS avg_speed_mph
        FROM gold.trip_duration_stats
        WHERE trip_date >= ? AND trip_date < ? {dow_filter}
        GROUP BY trip_hour
        ORDER BY trip_hour
        """,
        params,
    )


def durations_daily(day: date) -> Query:
    return (
        """
        SELECT trip_date, trip_hour AS hour, day_of_week, day_label, trips,
               round(avg_duration_min, 2) AS avg_duration_min,
               round(median_duration_min, 2) AS median_duration_min,
               round(p90_duration_min, 2) AS p90_duration_min,
               round(avg_distance_miles, 2) AS avg_distance_miles,
               round(avg_speed_mph, 2) AS avg_speed_mph
        FROM gold.trip_duration_stats
        WHERE trip_date = ?
        ORDER BY trip_hour
        """,
        [day],
    )


def _month_or_latest(table: str) -> str:
    # Sans mois demandé : le dernier mois disponible dans la table.
    return f"trip_month = coalesce(?, (SELECT max(trip_month) FROM gold.{table}))"


def top_areas(month: date | None, limit: int) -> Query:
    return (
        f"""
        SELECT trip_month, area_rank AS rank, community_area_id, community_area_name, side,
               trips,
               CAST(total_revenue AS double) AS total_revenue,
               CAST(avg_ticket AS double) AS avg_ticket,
               round(share_of_trips, 4) AS share_of_trips
        FROM gold.top_pickup_areas
        WHERE {_month_or_latest("top_pickup_areas")}
        ORDER BY area_rank
        LIMIT {int(limit)}
        """,
        # `limit` est un entier validé par FastAPI (1..100) : l'insérer est sans risque.
        [month],
    )


def area_history(community_area_id: int) -> Query:
    return (
        """
        SELECT trip_month, area_rank AS rank, community_area_id, community_area_name, side,
               trips,
               CAST(total_revenue AS double) AS total_revenue,
               CAST(avg_ticket AS double) AS avg_ticket,
               round(share_of_trips, 4) AS share_of_trips
        FROM gold.top_pickup_areas
        WHERE community_area_id = ?
        ORDER BY trip_month
        """,
        [community_area_id],
    )


def payment_mix(start: date, end: date) -> Query:
    return (
        """
        SELECT payment_type,
               sum(trips) AS trips,
               CAST(sum(total_revenue) AS double) AS total_revenue,
               round(CAST(sum(trips) AS double) / sum(sum(trips)) OVER (), 4) AS share_of_trips,
               round(CAST(sum(total_revenue) AS double)
                     / sum(CAST(sum(total_revenue) AS double)) OVER (), 4) AS share_of_revenue
        FROM gold.payment_mix
        WHERE trip_date >= ? AND trip_date < ?
        GROUP BY payment_type
        ORDER BY trips DESC
        """,
        [start, end],
    )


def companies(month: date | None, sort: CompanySort, limit: int, min_trips: int) -> Query:
    # `min_trips` : un ratio (ticket moyen, pourboire) calculé sur 50 trajets n'est pas
    # comparable à celui d'une flotte de 50 000 trajets.
    return (
        f"""
        SELECT trip_month, revenue_rank, company, trips, active_taxis,
               CAST(total_revenue AS double) AS total_revenue,
               CAST(avg_ticket AS double) AS avg_ticket,
               round(tip_rate, 4) AS tip_rate,
               round(avg_trip_miles, 2) AS avg_trip_miles
        FROM gold.company_performance
        WHERE {_month_or_latest("company_performance")}
          AND trips >= ?
        ORDER BY {_COMPANY_ORDER[sort]}, company
        LIMIT {int(limit)}
        """,
        [month, min_trips],
    )


def quality_latest() -> Query:
    return (
        """
        SELECT run_id, rule, severity, checked_rows, failed_rows,
               round(failed_pct, 4) AS failed_pct, checked_at
        FROM silver.dq_results
        WHERE run_id = (SELECT max_by(run_id, checked_at) FROM silver.dq_results)
        ORDER BY severity, rule
        """,
        [],
    )
