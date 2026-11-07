"""Écriture des fichiers bruts dans la landing zone (bucket bronze).

Layout : ``s3://bronze/landing/chicago_taxi/trip_date=YYYY-MM-DD/part-00000.csv``
          + ``_manifest.json`` écrit en DERNIER (sa présence = journée complète).

Une journée dont le manifest existe est sautée, sauf ``force=True`` : la reprise
après incident ne re-télécharge que ce qui manque.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from taxi_pipeline.config import Settings
from taxi_pipeline.extract.soda import SodaClient, day_where_clause
from taxi_pipeline.schemas import SOURCE_COLUMNS
from taxi_pipeline.storage import ObjectStore

log = logging.getLogger(__name__)

DATA_FILE = "part-00000.csv"
MANIFEST_FILE = "_manifest.json"


@dataclass(frozen=True)
class DayExtract:
    trip_date: str
    rows: int
    pages: int
    bytes: int
    key: str
    skipped: bool
    source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def daterange(start: date, end: date) -> list[date]:
    """Jours de ``[start, end)``."""
    return [start + timedelta(days=i) for i in range((end - start).days)]


def partition_prefix(settings: Settings, day: date) -> str:
    return f"{settings.landing_prefix}/trip_date={day.isoformat()}"


def landing_uri(settings: Settings, day: date, scheme: str = "s3a") -> str:
    return f"{scheme}://{settings.bucket_bronze}/{partition_prefix(settings, day)}/"


def _sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _publish(
    store: ObjectStore,
    settings: Settings,
    day: date,
    local_path: str,
    rows: int,
    pages: int,
    source: str,
    query: dict[str, Any],
) -> DayExtract:
    prefix = partition_prefix(settings, day)
    data_key = f"{prefix}/{DATA_FILE}"
    size = os.path.getsize(local_path)
    store.upload_file(local_path, settings.bucket_bronze, data_key)
    manifest = {
        "trip_date": day.isoformat(),
        "rows": rows,
        "pages": pages,
        "bytes": size,
        "sha256": _sha256(local_path),
        "file": f"s3://{settings.bucket_bronze}/{data_key}",
        "source": source,
        "query": query,
        "extracted_at": datetime.now(UTC).isoformat(),
    }
    store.put_json(settings.bucket_bronze, f"{prefix}/{MANIFEST_FILE}", manifest)
    log.info("Landing %s : %d lignes, %d pages, %d octets", day, rows, pages, size)
    return DayExtract(day.isoformat(), rows, pages, size, data_key, False, source)


def _skipped(store: ObjectStore, settings: Settings, day: date) -> DayExtract | None:
    prefix = partition_prefix(settings, day)
    manifest_key = f"{prefix}/{MANIFEST_FILE}"
    if not store.exists(settings.bucket_bronze, manifest_key):
        return None
    manifest = store.get_json(settings.bucket_bronze, manifest_key)
    log.info("Landing %s déjà complète (%s lignes) : saut", day, manifest.get("rows"))
    return DayExtract(
        trip_date=day.isoformat(),
        rows=int(manifest.get("rows", 0)),
        pages=int(manifest.get("pages", 0)),
        bytes=int(manifest.get("bytes", 0)),
        key=f"{prefix}/{DATA_FILE}",
        skipped=True,
        source=str(manifest.get("source", "unknown")),
    )


def extract_day_from_soda(
    client: SodaClient,
    store: ObjectStore,
    settings: Settings,
    day: date,
    force: bool = False,
) -> DayExtract:
    """Télécharge une journée (paginée) en streaming vers un fichier temporaire puis S3."""
    if not force and (done := _skipped(store, settings, day)):
        return done

    rows = pages = 0
    with tempfile.TemporaryDirectory(prefix="soda-") as tmp:
        local_path = os.path.join(tmp, DATA_FILE)
        with open(local_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
            writer.writerow(SOURCE_COLUMNS)
            for page in client.iter_pages(day):
                writer.writerows(page.rows)
                rows += len(page.rows)
                pages += 1
        query = {
            "url": client.url,
            "$select": ",".join(SOURCE_COLUMNS),
            "$where": day_where_clause(day),
            "$order": "trip_id",
            "$limit": client.page_size,
        }
        return _publish(store, settings, day, local_path, rows, pages, "soda", query)


def extract_days_from_sample(
    store: ObjectStore,
    settings: Settings,
    days: Iterable[date],
    sample_path: str,
    force: bool = False,
    only: date | None = None,
) -> list[DayExtract]:
    """Mode hors ligne : découpe le CSV d'exemple embarqué en partitions journalières.

    ``days`` est la période complète (sert à rattacher les lignes hors période au
    premier jour) ; ``only`` restreint l'écriture à un seul jour (tâche mappée).
    """
    day_list = list(days)
    if not day_list:
        return []
    known = {d.isoformat() for d in day_list}
    first_day = day_list[0].isoformat()

    by_day: dict[str, list[list[str]]] = defaultdict(list)
    with open(sample_path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if tuple(header) != SOURCE_COLUMNS:
            raise ValueError(f"En-tête inattendu dans {sample_path}")
        for row in reader:
            day_key = row[2][:10]
            # Timestamp absent / hors période : rattaché au 1er jour, la couche silver
            # le mettra en quarantaine (comme le ferait une donnée réelle mal datée).
            by_day[day_key if day_key in known else first_day].append(row)

    results: list[DayExtract] = []
    for day in [only] if only else day_list:
        if not force and (done := _skipped(store, settings, day)):
            results.append(done)
            continue
        day_rows = by_day.get(day.isoformat(), [])
        with tempfile.TemporaryDirectory(prefix="sample-") as tmp:
            local_path = os.path.join(tmp, DATA_FILE)
            with open(local_path, "w", encoding="utf-8", newline="") as out:
                writer = csv.writer(out, lineterminator="\n")
                writer.writerow(SOURCE_COLUMNS)
                writer.writerows(day_rows)
            results.append(
                _publish(
                    store,
                    settings,
                    day,
                    local_path,
                    len(day_rows),
                    1,
                    "sample",
                    {"file": sample_path},
                )
            )
    return results
