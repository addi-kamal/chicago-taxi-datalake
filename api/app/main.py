"""API REST de lecture de la couche gold (FastAPI -> Trino -> Iceberg).

Documentation interactive : http://localhost:8000/docs

Les endpoints sont en ``def`` (pas ``async def``) : le client Trino est bloquant,
FastAPI exécute donc chaque requête dans un pool de threads sans bloquer le serveur.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse

from app import queries
from app.db import Database, DataUnavailableError, get_db
from app.schemas import (
    CompanyPerformance,
    DailyDuration,
    DailyRevenue,
    Health,
    HourlyDuration,
    Kpis,
    Metadata,
    MonthlyRevenue,
    PaymentShare,
    PickupArea,
    QualityRule,
)

# Bornes ouvertes quand l'utilisateur ne précise pas de période : « toutes les données ».
_MIN_DAY = date(1900, 1, 1)
_MAX_DAY = date(2100, 1, 1)

app = FastAPI(
    title="Chicago Taxi - API gold",
    version="1.0.0",
    description=(
        "Lecture seule des indicateurs de la couche **gold** du datalake, via Trino.\n\n"
        "- Périodes : `start` inclus, `end` exclu (format `AAAA-MM-JJ`), comme le pipeline.\n"
        "- Mois : format `AAAA-MM` ; sans mois, le dernier mois disponible.\n"
        "- Ratios en fraction (0.124 = 12,4 %), montants en dollars US."
    ),
    openapi_tags=[
        {"name": "santé", "description": "Disponibilité de l'API et de Trino"},
        {"name": "revenus", "description": "CA, trajets, ticket moyen, pourboires"},
        {"name": "durées", "description": "Durées, distances et vitesses par heure"},
        {"name": "zones", "description": "Zones de prise en charge (community areas)"},
        {"name": "paiements", "description": "Répartition par moyen de paiement"},
        {"name": "compagnies", "description": "Performance mensuelle des compagnies"},
        {"name": "qualité", "description": "Résultats des contrôles qualité du dernier run"},
    ],
)

Db = Annotated[Database, Depends(get_db)]


@app.exception_handler(DataUnavailableError)
def _data_unavailable(_: Request, exc: DataUnavailableError) -> JSONResponse:
    return JSONResponse(status_code=503, content={"detail": str(exc)})


# --- Paramètres communs ---------------------------------------------------------------


@dataclass(frozen=True)
class Period:
    start: date
    end: date


def period(
    start: Annotated[date | None, Query(description="Premier jour (inclus)")] = None,
    end: Annotated[date | None, Query(description="Dernier jour (exclu)")] = None,
) -> Period:
    if start and end and start >= end:
        raise HTTPException(422, "`start` doit être strictement antérieur à `end`")
    return Period(start or _MIN_DAY, end or _MAX_DAY)


PeriodDep = Annotated[Period, Depends(period)]


def month(
    month: Annotated[
        str | None,
        Query(
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Mois `AAAA-MM` (défaut : dernier mois disponible)",
            examples=["2023-02"],
        ),
    ] = None,
) -> date | None:
    if month is None:
        return None
    year, mon = month.split("-")
    return date(int(year), int(mon), 1)


MonthDep = Annotated[date | None, Depends(month)]
Limit = Annotated[int, Query(ge=1, le=100, description="Nombre de lignes (1 à 100)")]


def _run(db: Database, query: queries.Query) -> list[dict[str, Any]]:
    sql, params = query
    return db.fetch(sql, params)


# --- Santé ------------------------------------------------------------------------------


@app.get("/health", response_model=Health, tags=["santé"])
def health() -> Health:
    """Liveness : le processus répond (utilisé par le healthcheck Docker)."""
    return Health(status="ok")


@app.get("/health/ready", response_model=Health, tags=["santé"])
def ready(db: Db) -> Health:
    """Readiness : Trino répond et peut exécuter une requête (503 sinon)."""
    _run(db, (queries.HEALTH, []))
    return Health(status="ok", trino="ok")


# --- Métadonnées ---------------------------------------------------------------------


@app.get("/v1/metadata", response_model=Metadata, tags=["revenus"])
def metadata(db: Db) -> Metadata:
    """Période couverte, volume total, mois disponibles et fraîcheur des données."""
    row = _run(db, queries.metadata())[0]
    months = [r["trip_month"] for r in _run(db, queries.available_months())]
    return Metadata(**row, months=months)


# --- Revenus -----------------------------------------------------------------------------


@app.get("/v1/kpis", response_model=Kpis, tags=["revenus"])
def kpis(db: Db, p: PeriodDep) -> Kpis:
    """KPIs de synthèse sur la période (mêmes chiffres que les tuiles du dashboard)."""
    return Kpis(**_run(db, queries.kpis(p.start, p.end))[0])


@app.get("/v1/revenue/daily", response_model=list[DailyRevenue], tags=["revenus"])
def revenue_daily(db: Db, p: PeriodDep) -> list[dict[str, Any]]:
    """Une ligne par jour : CA et ses composantes, ticket moyen, pourboires, taxis actifs."""
    return _run(db, queries.daily_revenue(p.start, p.end))


@app.get("/v1/revenue/monthly", response_model=list[MonthlyRevenue], tags=["revenus"])
def revenue_monthly(db: Db, p: PeriodDep) -> list[dict[str, Any]]:
    """Agrégat mensuel ; ticket moyen et taux de pourboire recalculés à partir des sommes."""
    return _run(db, queries.monthly_revenue(p.start, p.end))


# --- Durées ---------------------------------------------------------------------------


@app.get("/v1/durations/by-hour", response_model=list[HourlyDuration], tags=["durées"])
def durations_by_hour(
    db: Db,
    p: PeriodDep,
    day_of_week: Annotated[
        int | None, Query(ge=1, le=7, description="Jour ISO : 1 = lundi … 7 = dimanche")
    ] = None,
) -> list[dict[str, Any]]:
    """Profil horaire sur la période (moyennes pondérées par le nombre de trajets)."""
    return _run(db, queries.durations_by_hour(p.start, p.end, day_of_week))


@app.get("/v1/durations/daily", response_model=list[DailyDuration], tags=["durées"])
def durations_daily(
    db: Db, day: Annotated[date, Query(description="Jour `AAAA-MM-JJ`")]
) -> list[dict[str, Any]]:
    """Détail heure par heure d'un jour, avec médiane et p90 (non agrégeables entre jours)."""
    return _run(db, queries.durations_daily(day))


# --- Zones ------------------------------------------------------------------------------


@app.get("/v1/areas/top", response_model=list[PickupArea], tags=["zones"])
def top_areas(db: Db, m: MonthDep, limit: Limit = 10) -> list[dict[str, Any]]:
    """Classement des zones de prise en charge pour un mois."""
    return _run(db, queries.top_areas(m, limit))


@app.get("/v1/areas/{community_area_id}", response_model=list[PickupArea], tags=["zones"])
def area_history(
    db: Db,
    community_area_id: Annotated[int, Path(ge=1, le=77, description="Numéro de zone (1 à 77)")],
) -> list[dict[str, Any]]:
    """Historique mensuel d'une zone (rang, trajets, CA)."""
    rows = _run(db, queries.area_history(community_area_id))
    if not rows:
        raise HTTPException(404, f"Aucune donnée pour la zone {community_area_id}")
    return rows


# --- Paiements ------------------------------------------------------------------------


@app.get("/v1/payments/mix", response_model=list[PaymentShare], tags=["paiements"])
def payments_mix(db: Db, p: PeriodDep) -> list[dict[str, Any]]:
    """Part des trajets et du CA par moyen de paiement sur la période."""
    return _run(db, queries.payment_mix(p.start, p.end))


# --- Compagnies -----------------------------------------------------------------------


@app.get("/v1/companies", response_model=list[CompanyPerformance], tags=["compagnies"])
def companies(
    db: Db,
    m: MonthDep,
    sort: Annotated[queries.CompanySort, Query(description="Critère de tri")] = "revenue",
    limit: Limit = 20,
    min_trips: Annotated[
        int, Query(ge=0, description="Ignore les compagnies sous ce nombre de trajets")
    ] = 0,
) -> list[dict[str, Any]]:
    """Performance des compagnies pour un mois, triée par CA, trajets, ticket ou pourboire.

    Pour un tri sur un ratio (`avg_ticket`, `tip_rate`), fixer `min_trips` (ex. 1000) :
    sinon de très petites compagnies, peu représentatives, arrivent en tête.
    """
    return _run(db, queries.companies(m, sort, limit, min_trips))


# --- Qualité ----------------------------------------------------------------------------


@app.get("/v1/quality/latest", response_model=list[QualityRule], tags=["qualité"])
def quality_latest(db: Db) -> list[dict[str, Any]]:
    """Règles qualité du dernier run : permet à un consommateur de juger la fiabilité."""
    return _run(db, queries.quality_latest())
