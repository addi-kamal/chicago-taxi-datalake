"""Téléchargement reproductible d'une tranche du dataset Chicago Taxi Trips (hors orchestrateur).

Même code que la tâche Airflow `extract_day` (module ``taxi_pipeline.extract``) :
une partition par jour, pagination stable (``$order=trip_id``), manifest par jour,
reprise sans re-téléchargement.

Exemples :
    # Volume attendu, sans rien télécharger
    python scripts/download_taxi_data.py --start 2023-01-01 --end 2023-04-01 --count-only

    # Vers un dossier local (./data/landing/chicago_taxi/trip_date=YYYY-MM-DD/)
    python scripts/download_taxi_data.py --start 2023-01-01 --end 2023-04-01 --local-dir data

    # Vers le datalake (MinIO/Ceph, variables S3_* de l'environnement)
    docker compose exec airflow-scheduler \\
        python /opt/airflow/project/scripts/download_taxi_data.py \
        --start 2023-01-01 --end 2023-01-08
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.config import Settings
from taxi_pipeline.extract.landing import daterange, extract_day_from_soda
from taxi_pipeline.extract.soda import SodaClient
from taxi_pipeline.storage import ObjectStore, S3ObjectStore


class LocalStore:
    """Implémentation fichier de ``ObjectStore`` : bucket = sous-dossier."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / key if bucket == "bronze" else self.root / bucket / key

    def exists(self, bucket: str, key: str) -> bool:
        return self._path(bucket, key).exists()

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        target = self._path(bucket, key)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:
        target = self._path(bucket, key)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")

    def get_json(self, bucket: str, key: str) -> dict[str, Any]:
        data: dict[str, Any] = json.loads(self._path(bucket, key).read_text(encoding="utf-8"))
        return data


def count_rows(settings: Settings, start: date, end: date) -> int:
    client = SodaClient(
        settings.soda_base_url, settings.soda_dataset_id, settings.soda_app_token, timeout_s=300
    )
    url = client.url.replace(".csv", ".json")
    where = (
        f"trip_start_timestamp >= '{start}T00:00:00' AND trip_start_timestamp < '{end}T00:00:00'"
    )
    resp = client.session.get(
        url, params={"$select": "count(*) AS n", "$where": where}, timeout=300
    )
    resp.raise_for_status()
    return int(resp.json()[0]["n"])


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--start", required=True, type=date.fromisoformat, help="inclus")
    parser.add_argument("--end", required=True, type=date.fromisoformat, help="exclu")
    parser.add_argument("--local-dir", type=Path, help="écrit en local au lieu de S3")
    parser.add_argument("--force", action="store_true", help="re-télécharge les jours présents")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--count-only", action="store_true", help="affiche le volume et sort")
    args = parser.parse_args()

    settings = Settings(pipeline_start_date=args.start, pipeline_end_date=args.end)
    if args.count_only:
        n = count_rows(settings, args.start, args.end)
        print(f"{n:,} trajets entre {args.start} et {args.end}")
        return 0

    store: ObjectStore = LocalStore(args.local_dir) if args.local_dir else S3ObjectStore(settings)
    client = SodaClient(
        settings.soda_base_url,
        settings.soda_dataset_id,
        settings.soda_app_token or os.environ.get("SODA_APP_TOKEN"),
        settings.soda_page_size,
        settings.soda_timeout_s,
    )

    def one(day: date) -> dict[str, Any]:
        return extract_day_from_soda(client, store, settings, day, args.force).to_dict()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(one, daterange(args.start, args.end)))
    total = sum(r["rows"] for r in results)
    skipped = sum(r["skipped"] for r in results)
    print(f"{len(results)} jours ({skipped} déjà présents), {total:,} lignes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
