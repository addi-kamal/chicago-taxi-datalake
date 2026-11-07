"""Client SODA (Socrata Open Data API) paginé pour le dataset Taxi Trips.

Garanties :
- `$select` explicite : ordre des colonnes déterministe, contrôlé par le pipeline ;
- `$order=trip_id` : ordre stable, indispensable pour une pagination par offset fiable ;
- en-tête vérifié à chaque page : échec explicite si une colonne attendue manque ;
- retries HTTP avec backoff exponentiel (429 / 5xx).
"""

from __future__ import annotations

import csv
import io
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from taxi_pipeline.schemas import SOURCE_COLUMNS

log = logging.getLogger(__name__)


class SourceSchemaError(RuntimeError):
    """L'en-tête renvoyé par la source ne correspond pas au contrat attendu."""


class SourceCompletenessError(RuntimeError):
    """Le nombre de lignes reçues ne correspond pas au count(*) de la source."""


@dataclass(frozen=True)
class SodaPage:
    offset: int
    rows: list[list[str]]


def day_where_clause(day: date) -> str:
    start = day.isoformat()
    end = (day + timedelta(days=1)).isoformat()
    return f"trip_start_timestamp >= '{start}T00:00:00' AND trip_start_timestamp < '{end}T00:00:00'"


class SodaClient:
    def __init__(
        self,
        base_url: str,
        dataset_id: str,
        app_token: str | None = None,
        page_size: int = 50_000,
        timeout_s: int = 180,
        session: requests.Session | None = None,
    ) -> None:
        self.url = f"{base_url.rstrip('/')}/{dataset_id}.csv"
        self.page_size = page_size
        self.timeout_s = timeout_s
        self.session = session or self._build_session()
        if app_token:
            self.session.headers["X-App-Token"] = app_token
        self.session.headers.setdefault("User-Agent", "taxi-pipeline/1.0 (data-engineering POC)")

    @staticmethod
    def _build_session() -> requests.Session:
        retry = Retry(
            total=5,
            backoff_factor=2.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        return session

    def query_params(self, day: date, offset: int) -> dict[str, str]:
        return {
            "$select": ",".join(SOURCE_COLUMNS),
            "$where": day_where_clause(day),
            "$order": "trip_id",
            "$limit": str(self.page_size),
            "$offset": str(offset),
        }

    def fetch_page(self, day: date, offset: int) -> SodaPage:
        resp = self.session.get(
            self.url, params=self.query_params(day, offset), timeout=self.timeout_s
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        reader = csv.reader(io.StringIO(resp.text))
        header = next(reader, None)
        validate_header(header)
        rows = [row for row in reader if row]
        return SodaPage(offset=offset, rows=rows)

    def count_day(self, day: date) -> int:
        """Nombre de trajets de la journée selon la source (count(*))."""
        resp = self.session.get(
            self.url,
            params={"$select": "count(*)", "$where": day_where_clause(day)},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        reader = csv.reader(io.StringIO(resp.text))
        next(reader, None)  # en-tête
        row = next(reader, None)
        if not row:
            raise SourceSchemaError("Réponse count(*) vide")
        return int(row[0])

    def iter_pages(self, day: date) -> Iterator[SodaPage]:
        """Itère sur les pages d'une journée jusqu'à une page incomplète."""
        offset = 0
        while True:
            page = self.fetch_page(day, offset)
            log.info("SODA %s offset=%d -> %d lignes", day, offset, len(page.rows))
            yield page
            if len(page.rows) < self.page_size:
                return
            offset += self.page_size


def validate_header(header: list[str] | None) -> None:
    if header is None:
        raise SourceSchemaError("Réponse SODA vide : aucun en-tête CSV reçu")
    missing = [c for c in SOURCE_COLUMNS if c not in header]
    if missing:
        raise SourceSchemaError(f"Colonnes attendues absentes de la source : {missing}")
    if tuple(header) != SOURCE_COLUMNS:
        raise SourceSchemaError(
            f"Ordre/colonnes inattendus dans la source. "
            f"Attendu {list(SOURCE_COLUMNS)}, reçu {header}"
        )
