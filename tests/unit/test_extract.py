"""Extraction SODA -> landing : pagination, contrat d'en-tête, manifest, reprise."""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import Any

import pytest

from taxi_pipeline.config import Settings
from taxi_pipeline.extract.landing import (
    MANIFEST_FILE,
    extract_day_from_soda,
    extract_days_from_sample,
)
from taxi_pipeline.extract.soda import SodaClient, SourceSchemaError, day_where_clause
from taxi_pipeline.schemas import SOURCE_COLUMNS
from tests.conftest import SAMPLE_CSV


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.encoding = "utf-8"
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    """Simule l'API SODA : renvoie `total` lignes, paginées selon $limit/$offset."""

    def __init__(self, total: int, header: list[str] | None = None) -> None:
        self.total = total
        self.header = header or list(SOURCE_COLUMNS)
        self.headers: dict[str, str] = {}
        self.calls: list[dict[str, str]] = []

    def get(self, url: str, params: dict[str, str], timeout: int) -> FakeResponse:
        self.calls.append(params)
        offset, limit = int(params["$offset"]), int(params["$limit"])
        buf = io.StringIO()
        writer = csv.writer(buf, quoting=csv.QUOTE_ALL)
        writer.writerow(self.header)
        for i in range(offset, min(offset + limit, self.total)):
            writer.writerow([f"id{i:05d}", *["x"] * (len(self.header) - 1)])
        return FakeResponse(buf.getvalue())


class FakeStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], Any] = {}

    def exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self.objects

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        with open(local_path, encoding="utf-8") as fh:
            self.objects[(bucket, key)] = fh.read()

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:
        self.objects[(bucket, key)] = payload

    def get_json(self, bucket: str, key: str) -> dict[str, Any]:
        data: dict[str, Any] = self.objects[(bucket, key)]
        return data


SETTINGS = Settings(sample_data_path=str(SAMPLE_CSV))
DAY = date(2023, 1, 10)


def _client(session: FakeSession, page_size: int = 100) -> SodaClient:
    return SodaClient("https://example.org/resource", "wrvz-psew", None, page_size, 10, session)  # type: ignore[arg-type]


def test_where_clause_covers_exactly_one_day() -> None:
    assert day_where_clause(DAY) == (
        "trip_start_timestamp >= '2023-01-10T00:00:00' "
        "AND trip_start_timestamp < '2023-01-11T00:00:00'"
    )


def test_pagination_reads_all_rows_with_stable_order() -> None:
    session = FakeSession(total=250)
    store = FakeStore()
    result = extract_day_from_soda(_client(session), store, SETTINGS, DAY)

    assert result.rows == 250
    assert result.pages == 3  # 100 + 100 + 50 (page incomplète = fin)
    assert [c["$offset"] for c in session.calls] == ["0", "100", "200"]
    assert all(c["$order"] == "trip_id" for c in session.calls)
    content = store.objects[("bronze", result.key)]
    assert content.count("\n") == 251  # en-tête + 250 lignes, un seul en-tête
    assert content.startswith(",".join(SOURCE_COLUMNS))


def test_exact_multiple_of_page_size_ends_on_empty_page() -> None:
    session = FakeSession(total=200)
    result = extract_day_from_soda(_client(session), FakeStore(), SETTINGS, DAY)
    assert result.rows == 200
    assert result.pages == 3


def test_manifest_written_and_day_skipped_on_rerun() -> None:
    session, store = FakeSession(total=10), FakeStore()
    first = extract_day_from_soda(_client(session), store, SETTINGS, DAY)
    manifest_key = first.key.rsplit("/", 1)[0] + "/" + MANIFEST_FILE
    assert store.get_json("bronze", manifest_key)["rows"] == 10

    calls_before = len(session.calls)
    second = extract_day_from_soda(_client(session), store, SETTINGS, DAY)
    assert second.skipped and second.rows == 10
    assert len(session.calls) == calls_before  # aucun nouvel appel réseau

    forced = extract_day_from_soda(_client(session), store, SETTINGS, DAY, force=True)
    assert not forced.skipped and len(session.calls) > calls_before


def test_missing_source_column_fails_explicitly() -> None:
    header = [c for c in SOURCE_COLUMNS if c != "fare"]
    with pytest.raises(SourceSchemaError, match="fare"):
        extract_day_from_soda(_client(FakeSession(5, header)), FakeStore(), SETTINGS, DAY)


def test_sample_mode_splits_by_day_and_keeps_all_rows() -> None:
    store = FakeStore()
    days = [date(2023, 1, 1), date(2023, 1, 2), date(2023, 1, 3)]
    results = extract_days_from_sample(store, SETTINGS, days, str(SAMPLE_CSV))
    with open(SAMPLE_CSV, encoding="utf-8") as fh:
        total = sum(1 for _ in fh) - 1
    assert [r.trip_date for r in results] == [d.isoformat() for d in days]
    assert sum(r.rows for r in results) == total

    # Tâche mappée : un seul jour écrit, mêmes volumes
    single = extract_days_from_sample(
        FakeStore(), SETTINGS, days, str(SAMPLE_CSV), only=date(2023, 1, 2)
    )
    assert len(single) == 1 and single[0].rows == results[1].rows
