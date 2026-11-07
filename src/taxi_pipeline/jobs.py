"""Points d'entrée des étapes du pipeline, indépendants de l'orchestrateur.

Chaque fonction prend et retourne des types simples (str, int, dict, list) :
- sérialisables (XCom Airflow, résultats Prefect/Dagster, CLI) ;
- jamais de DataFrame ni de SparkSession échangés entre étapes.

Chaque étape Spark ouvre et ferme sa propre SparkSession.
Utilisable aussi en CLI : ``python -m taxi_pipeline.jobs <étape> --start ... --end ...``
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict
from datetime import date
from typing import Any

from taxi_pipeline.config import Settings, get_settings
from taxi_pipeline.extract.landing import (
    daterange,
    extract_day_from_soda,
    extract_days_from_sample,
)
from taxi_pipeline.extract.soda import SodaClient
from taxi_pipeline.spark import spark_session
from taxi_pipeline.storage import S3ObjectStore

log = logging.getLogger(__name__)


def resolve_period(
    start: str | None, end: str | None, settings: Settings | None = None
) -> tuple[date, date]:
    s = settings or get_settings()
    start_d = date.fromisoformat(start) if start else s.pipeline_start_date
    end_d = date.fromisoformat(end) if end else s.pipeline_end_date
    if end_d <= start_d:
        raise ValueError(f"Période invalide : {start_d} -> {end_d} (fin exclue)")
    return start_d, end_d


def plan_days(start: str | None = None, end: str | None = None) -> list[str]:
    start_d, end_d = resolve_period(start, end)
    return [d.isoformat() for d in daterange(start_d, end_d)]


def extract_day(day: str, start: str, end: str, force: bool = False) -> dict[str, Any]:
    settings = get_settings()
    store = S3ObjectStore(settings)
    target = date.fromisoformat(day)
    if settings.pipeline_source == "sample":
        start_d, end_d = resolve_period(start, end, settings)
        result = extract_days_from_sample(
            store, settings, daterange(start_d, end_d), settings.sample_data_path, force, target
        )[0]
    else:
        client = SodaClient(
            settings.soda_base_url,
            settings.soda_dataset_id,
            settings.soda_app_token,
            settings.soda_page_size,
            settings.soda_timeout_s,
        )
        result = extract_day_from_soda(client, store, settings, target, force)
    return result.to_dict()


def load_bronze(days: list[str], run_id: str) -> dict[str, int]:
    from taxi_pipeline.bronze.load import load_bronze as _load

    settings = get_settings()
    with spark_session(settings, "bronze-load") as spark:
        return _load(spark, settings, [date.fromisoformat(d) for d in days], run_id)


def load_reference_data() -> dict[str, int]:
    from taxi_pipeline.silver.load import load_community_areas

    settings = get_settings()
    with spark_session(settings, "silver-reference") as spark:
        return load_community_areas(spark, settings)


def transform_silver(start: str, end: str, run_id: str) -> dict[str, Any]:
    from taxi_pipeline.silver.load import transform_silver as _transform

    settings = get_settings()
    start_d, end_d = resolve_period(start, end, settings)
    with spark_session(settings, "silver-transform") as spark:
        return _transform(spark, settings, start_d, end_d, run_id)


def build_gold(start: str, end: str) -> dict[str, int]:
    from taxi_pipeline.gold.build import build_gold as _build

    settings = get_settings()
    start_d, end_d = resolve_period(start, end, settings)
    with spark_session(settings, "gold-build") as spark:
        return _build(spark, settings, start_d, end_d)


def validate_serving(
    start: str, end: str, expected_silver_rows: int | None
) -> list[dict[str, Any]]:
    from taxi_pipeline.serving.trino_checks import validate_serving_layer

    settings = get_settings()
    start_d, end_d = resolve_period(start, end, settings)
    return [
        asdict(r) for r in validate_serving_layer(settings, start_d, end_d, expected_silver_rows)
    ]


def publish_dashboard() -> dict[str, str]:
    from taxi_pipeline.bi.superset import provision_dashboard

    return provision_dashboard(get_settings())


def run_maintenance(snapshot_retention_days: int = 7) -> dict[str, Any]:
    from taxi_pipeline.maintenance.iceberg import maintain_tables

    settings = get_settings()
    with spark_session(settings, "iceberg-maintenance") as spark:
        return maintain_tables(spark, settings, snapshot_retention_days)


# --- CLI (exécution hors orchestrateur) ------------------------------------------------------


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taxi_pipeline.jobs", description=__doc__)
    parser.add_argument(
        "step",
        choices=["extract", "bronze", "reference", "silver", "gold", "serving", "dashboard", "all"],
    )
    parser.add_argument("--start", help="YYYY-MM-DD (inclus)")
    parser.add_argument("--end", help="YYYY-MM-DD (exclu)")
    parser.add_argument(
        "--force", action="store_true", help="re-télécharge les jours déjà présents"
    )
    parser.add_argument("--run-id", default="cli")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    start_d, end_d = resolve_period(args.start, args.end)
    start, end = start_d.isoformat(), end_d.isoformat()
    days = plan_days(start, end)
    steps = (
        ["extract", "bronze", "reference", "silver", "gold", "serving", "dashboard"]
        if args.step == "all"
        else [args.step]
    )
    silver_rows: int | None = None
    for step in steps:
        out: Any
        if step == "extract":
            out = [extract_day(d, start, end, args.force) for d in days]
        elif step == "bronze":
            out = load_bronze(days, args.run_id)
        elif step == "reference":
            out = load_reference_data()
        elif step == "silver":
            out = transform_silver(start, end, args.run_id)
            silver_rows = out["valid_rows"]
            if not out["gate_passed"]:
                raise SystemExit(out["gate_message"])
        elif step == "gold":
            out = build_gold(start, end)
        elif step == "serving":
            out = validate_serving(start, end, silver_rows)
        else:
            out = publish_dashboard()
        print(json.dumps({step: out}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
