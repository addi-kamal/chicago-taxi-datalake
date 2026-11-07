"""Tests de l'API sans Trino : la base est remplacée par une fausse implémentation.

On vérifie le contrat HTTP (codes, validation, formats) et que les valeurs saisies
par l'utilisateur partent bien en paramètres SQL, jamais dans le texte de la requête.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from datetime import date
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.db import DataUnavailableError, get_db
from app.main import app


class FakeDb:
    def __init__(self, rows: list[dict[str, Any]] | None = None, fail: bool = False) -> None:
        self.rows = rows or []
        self.fail = fail
        self.calls: list[tuple[str, list[Any]]] = []

    def fetch(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        self.calls.append((sql, list(params)))
        if self.fail:
            raise DataUnavailableError("Trino est injoignable")
        return self.rows


@pytest.fixture
def db() -> Iterator[FakeDb]:
    fake = FakeDb()
    app.dependency_overrides[get_db] = lambda: fake
    yield fake
    app.dependency_overrides.clear()


@pytest.fixture
def client(db: FakeDb) -> TestClient:
    return TestClient(app)


KPI_ROW = {
    "first_day": date(2023, 1, 1),
    "last_day": date(2023, 3, 31),
    "days": 90,
    "trips": 1_446_430,
    "total_revenue": 36_942_000.5,
    "avg_ticket": 25.54,
    "tip_rate": 0.124,
}


def test_health_does_not_query_trino(client: TestClient, db: FakeDb) -> None:
    assert client.get("/health").json() == {"status": "ok", "trino": None}
    assert db.calls == []


def test_ready_returns_503_when_trino_down(client: TestClient, db: FakeDb) -> None:
    db.fail = True
    response = client.get("/health/ready")
    assert response.status_code == 503
    assert "injoignable" in response.json()["detail"]


def test_kpis_passes_period_as_parameters(client: TestClient, db: FakeDb) -> None:
    db.rows = [KPI_ROW]
    response = client.get("/v1/kpis", params={"start": "2023-01-01", "end": "2023-02-01"})
    assert response.status_code == 200
    assert response.json()["trips"] == 1_446_430
    sql, params = db.calls[0]
    assert params == [date(2023, 1, 1), date(2023, 2, 1)]
    assert "2023-01-01" not in sql


def test_kpis_without_period_uses_open_bounds(client: TestClient, db: FakeDb) -> None:
    db.rows = [KPI_ROW]
    client.get("/v1/kpis")
    assert db.calls[0][1] == [date(1900, 1, 1), date(2100, 1, 1)]


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/kpis", {"start": "2023-02-01", "end": "2023-01-01"}),  # start >= end
        ("/v1/kpis", {"start": "pas-une-date"}),
        ("/v1/areas/top", {"month": "2023-13"}),
        ("/v1/areas/top", {"limit": 0}),
        ("/v1/areas/top", {"limit": 101}),
        ("/v1/companies", {"sort": "company; DROP TABLE x"}),
        ("/v1/durations/by-hour", {"day_of_week": 8}),
        ("/v1/areas/78", {}),
    ],
)
def test_invalid_inputs_are_rejected_before_sql(
    client: TestClient, db: FakeDb, path: str, params: dict[str, Any]
) -> None:
    assert client.get(path, params=params).status_code == 422
    assert db.calls == []


def test_month_is_converted_to_first_day(client: TestClient, db: FakeDb) -> None:
    client.get("/v1/areas/top", params={"month": "2023-02", "limit": 5})
    sql, params = db.calls[0]
    assert params == [date(2023, 2, 1)]
    assert "LIMIT 5" in sql


def test_month_defaults_to_latest(client: TestClient, db: FakeDb) -> None:
    client.get("/v1/companies")
    sql, params = db.calls[0]
    assert params == [None, 0]
    assert "max(trip_month)" in sql


def test_companies_min_trips_is_a_parameter(client: TestClient, db: FakeDb) -> None:
    client.get("/v1/companies", params={"month": "2023-03", "min_trips": 1000})
    assert db.calls[0][1] == [date(2023, 3, 1), 1000]
    assert client.get("/v1/companies", params={"min_trips": -1}).status_code == 422


def test_company_sort_uses_whitelist(client: TestClient, db: FakeDb) -> None:
    client.get("/v1/companies", params={"sort": "tip_rate"})
    assert "ORDER BY tip_rate DESC" in db.calls[0][0]


def test_day_of_week_filter_is_optional(client: TestClient, db: FakeDb) -> None:
    client.get("/v1/durations/by-hour")
    client.get("/v1/durations/by-hour", params={"day_of_week": 5})
    (sql_all, params_all), (sql_dow, params_dow) = db.calls
    assert "day_of_week" not in sql_all
    assert params_dow[-1] == 5


def test_unknown_area_returns_404(client: TestClient, db: FakeDb) -> None:
    assert client.get("/v1/areas/12").status_code == 404


def test_openapi_lists_all_endpoints(client: TestClient) -> None:
    paths = set(client.get("/openapi.json").json()["paths"])
    assert {
        "/health",
        "/health/ready",
        "/v1/metadata",
        "/v1/kpis",
        "/v1/revenue/daily",
        "/v1/revenue/monthly",
        "/v1/durations/by-hour",
        "/v1/durations/daily",
        "/v1/areas/top",
        "/v1/areas/{community_area_id}",
        "/v1/payments/mix",
        "/v1/companies",
        "/v1/quality/latest",
    } <= paths
