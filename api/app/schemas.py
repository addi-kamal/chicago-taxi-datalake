"""Contrats de réponse de l'API (documentés automatiquement dans /docs).

Convention : les ratios (``tip_rate``, ``share_of_*``) sont des fractions entre 0 et 1
(0.124 = 12,4 %) ; les montants sont en dollars US.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class Health(BaseModel):
    status: str
    trino: str | None = None


class Metadata(BaseModel):
    first_day: date | None = Field(description="Premier jour disponible en gold")
    last_day: date | None = Field(description="Dernier jour disponible en gold")
    days: int
    trips: int
    months: list[date]
    data_updated_at: datetime | None = Field(
        description="Date du dernier commit Iceberg de gold.daily_revenue (fraîcheur)"
    )


class Kpis(BaseModel):
    first_day: date | None
    last_day: date | None
    days: int
    trips: int
    total_revenue: float
    avg_ticket: float | None = Field(description="CA / nombre de trajets")
    tip_rate: float | None = Field(description="Pourboires / tarifs (fraction)")


class DailyRevenue(BaseModel):
    trip_date: date
    trips: int
    total_revenue: float
    fare_revenue: float
    tips_revenue: float
    tolls_revenue: float
    extras_revenue: float
    avg_ticket: float | None
    tip_rate: float | None
    active_taxis: int


class MonthlyRevenue(BaseModel):
    trip_month: date
    days: int
    trips: int
    total_revenue: float
    avg_ticket: float | None
    tip_rate: float | None


class HourlyDuration(BaseModel):
    hour: int
    trips: int
    avg_duration_min: float | None
    avg_distance_miles: float | None
    avg_speed_mph: float | None


class DailyDuration(BaseModel):
    trip_date: date
    hour: int
    day_of_week: int
    day_label: str
    trips: int
    avg_duration_min: float | None
    median_duration_min: float | None
    p90_duration_min: float | None
    avg_distance_miles: float | None
    avg_speed_mph: float | None


class PickupArea(BaseModel):
    trip_month: date
    rank: int
    community_area_id: int
    community_area_name: str | None
    side: str | None
    trips: int
    total_revenue: float
    avg_ticket: float | None
    share_of_trips: float | None


class PaymentShare(BaseModel):
    payment_type: str | None
    trips: int
    total_revenue: float
    share_of_trips: float | None
    share_of_revenue: float | None


class CompanyPerformance(BaseModel):
    trip_month: date
    revenue_rank: int
    company: str | None
    trips: int
    active_taxis: int
    total_revenue: float
    avg_ticket: float | None
    tip_rate: float | None
    avg_trip_miles: float | None


class QualityRule(BaseModel):
    run_id: str
    rule: str
    severity: str
    checked_rows: int
    failed_rows: int
    failed_pct: float
    checked_at: datetime
