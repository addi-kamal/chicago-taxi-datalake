"""
### Chicago Taxi Trips - pipeline médaillon de bout en bout

`SODA API → landing (S3) → bronze → silver (+ quarantaine, DQ) → gold → Trino → Superset`

| Étape | Moteur | Sortie |
|---|---|---|
| `extract_day` (1 tâche / jour) | Python | `s3://bronze/landing/chicago_taxi/trip_date=…/` |
| `load_bronze` | Spark | `iceberg.bronze.taxi_trips_raw` |
| `load_reference_data` | Spark | `iceberg.silver.dim_community_area` |
| `transform_silver` | Spark | `iceberg.silver.taxi_trips`, `taxi_trips_rejected`, `dq_results` |
| `quality_gate` | Python | échec si taux de rejet > `DQ_MAX_REJECT_PCT` |
| `build_gold` | Spark | 5 tables `iceberg.gold.*` |
| `validate_serving_layer` | Trino | Trino relit silver/gold (mêmes volumes que Spark) |
| `publish_dashboard` | Superset API | dashboards `chicago-taxi` et `chicago-taxi-quality` |

Planifié `@once` et actif dès sa création : il se lance **automatiquement** au premier
`docker compose up`. Pour une autre période : *Trigger DAG w/ config*.
Toutes les étapes sont idempotentes (relance = mêmes résultats, sans doublon).
"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import get_current_context

DEFAULT_START = os.environ.get("PIPELINE_START_DATE", "2023-01-01")
DEFAULT_END = os.environ.get("PIPELINE_END_DATE", "2023-01-08")
EXTRACT_PARALLELISM = int(os.environ.get("EXTRACT_PARALLELISM", "4"))
SPARK_POOL = "spark"  # 1 slot : les jobs Spark locaux s'exécutent l'un après l'autre
# Étapes Spark idempotentes : un retry est sans danger (erreur transitoire S3, OOM...).
# Les AirflowFailException (écart landing/bronze, gate qualité) ne sont jamais rejouées.
SPARK_RETRY: dict[str, Any] = {"retries": 1, "retry_delay": timedelta(minutes=1)}


@dag(
    dag_id="chicago_taxi_e2e",
    description="Ingestion SODA -> bronze -> silver -> gold -> Trino -> Superset",
    schedule="@once",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args={"owner": "data-platform", "retries": 0},
    params={
        "start_date": Param(DEFAULT_START, type="string", format="date", title="Début (inclus)"),
        "end_date": Param(DEFAULT_END, type="string", format="date", title="Fin (exclue)"),
        "force": Param(False, type="boolean", title="Re-télécharger les jours déjà présents"),
    },
    tags=["chicago-taxi", "medallion", "iceberg", "spark", "trino"],
    doc_md=__doc__,
)
def chicago_taxi_e2e() -> None:
    @task(multiple_outputs=True)
    def plan_period() -> dict[str, Any]:
        from taxi_pipeline.jobs import plan_days, resolve_period

        params = get_current_context()["params"]
        start, end = resolve_period(params["start_date"], params["end_date"])
        days = plan_days(start.isoformat(), end.isoformat())
        print(f"Période {start} -> {end} (exclue) : {len(days)} jour(s)")
        return {"start": start.isoformat(), "end": end.isoformat(), "force": bool(params["force"])}

    @task
    def list_days(start: str, end: str) -> list[str]:
        """Liste des jours à extraire (Airflow ne mappe que sur une valeur de retour complète)."""
        from taxi_pipeline.jobs import plan_days

        return plan_days(start, end)

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_active_tis_per_dagrun=EXTRACT_PARALLELISM,
        execution_timeout=timedelta(minutes=30),
        map_index_template="{{ trip_day }}",
    )
    def extract_day(day: str, start: str, end: str, force: bool) -> dict[str, Any]:
        from taxi_pipeline.jobs import extract_day as _extract

        get_current_context()["trip_day"] = day  # libellé lisible de la tâche mappée dans l'UI
        return _extract(day, start, end, force)

    @task(pool=SPARK_POOL, execution_timeout=timedelta(minutes=45), **SPARK_RETRY)
    def load_bronze(manifests: list[dict[str, Any]]) -> dict[str, int]:
        from taxi_pipeline.jobs import load_bronze as _load

        manifests = list(manifests)
        downloaded = sum(m["rows"] for m in manifests)
        print(f"Landing : {len(manifests)} jour(s), {downloaded} lignes source")
        result = _load([m["trip_date"] for m in manifests], get_current_context()["run_id"])
        if result["rows"] != downloaded:
            raise AirflowFailException(
                f"Écart landing/bronze : {downloaded} lignes téléchargées, "
                f"{result['rows']} chargées"
            )
        return result

    @task(pool=SPARK_POOL, execution_timeout=timedelta(minutes=15), **SPARK_RETRY)
    def load_reference_data() -> dict[str, int]:
        from taxi_pipeline.jobs import load_reference_data as _ref

        return _ref()

    @task(pool=SPARK_POOL, execution_timeout=timedelta(minutes=60), **SPARK_RETRY)
    def transform_silver(start: str, end: str) -> dict[str, Any]:
        from taxi_pipeline.jobs import transform_silver as _silver

        return _silver(start, end, get_current_context()["run_id"])

    @task
    def quality_gate(silver: dict[str, Any]) -> dict[str, Any]:
        lines = [f"{'règle':<32} {'sévérité':<9} {'contrôlées':>12} {'en échec':>10} {'%':>9}"]
        for r in silver["dq_results"]:
            lines.append(
                f"{r['rule']:<32} {r['severity']:<9} {r['checked_rows']:>12} "
                f"{r['failed_rows']:>10} {r['failed_pct']:>9.3f}"
            )
        print("Résultats qualité :\n" + "\n".join(lines))
        print(silver["gate_message"])
        if not silver["gate_passed"]:
            raise AirflowFailException(silver["gate_message"])
        return {"valid_rows": silver["valid_rows"], "reject_pct": silver["reject_pct"]}

    @task(pool=SPARK_POOL, execution_timeout=timedelta(minutes=45), **SPARK_RETRY)
    def build_gold(start: str, end: str) -> dict[str, int]:
        from taxi_pipeline.jobs import build_gold as _gold

        return _gold(start, end)

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def validate_serving_layer(start: str, end: str, gate: dict[str, Any]) -> list[dict[str, Any]]:
        from taxi_pipeline.jobs import validate_serving

        return validate_serving(start, end, int(gate["valid_rows"]))

    @task(retries=3, retry_delay=timedelta(minutes=1))
    def publish_dashboard() -> dict[str, str]:
        from taxi_pipeline.jobs import publish_dashboard as _publish

        return _publish()

    @task
    def report(
        bronze: dict[str, int],
        gate: dict[str, Any],
        gold: dict[str, int],
        checks: list[dict[str, Any]],
        dashboards: dict[str, str],
    ) -> None:
        print("=" * 72)
        print("PIPELINE TERMINÉ")
        print(f"  bronze  : {bronze['rows']:>12,} lignes ({bronze['days']} jours)")
        print(
            f"  silver  : {gate['valid_rows']:>12,} lignes valides (rejet {gate['reject_pct']} %)"
        )
        for table, n in gold.items():
            print(f"  gold    : {table:<22} {n:>8,} lignes")
        print(f"  trino   : {sum(c['passed'] for c in checks)}/{len(checks)} contrôles OK")
        for name, dash_url in dashboards.items():
            print(f"  dashboard {name:<8}: {dash_url}")
        print("=" * 72)

    period = plan_period()
    manifests = extract_day.partial(
        start=period["start"], end=period["end"], force=period["force"]
    ).expand(day=list_days(period["start"], period["end"]))
    bronze = load_bronze(manifests)
    reference = load_reference_data()
    silver = transform_silver(period["start"], period["end"])
    gate = quality_gate(silver)
    gold = build_gold(period["start"], period["end"])
    checks = validate_serving_layer(period["start"], period["end"], gate)
    dashboards = publish_dashboard()

    bronze >> reference >> silver
    gate >> gold >> checks >> dashboards
    report(bronze, gate, gold, checks, dashboards)


chicago_taxi_e2e()
