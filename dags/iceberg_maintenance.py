"""
### Maintenance hebdomadaire des tables Iceberg (Spark)

Compaction (`rewrite_data_files`), `rewrite_manifests`, expiration des snapshots
(rétention paramétrable) et suppression des fichiers orphelins (> 3 jours).

Un seul moteur porte la maintenance : Spark (Trino reste en lecture).
DAG **en pause à la création** : l'activer depuis l'UI Airflow une fois les premières
données chargées.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


@dag(
    dag_id="iceberg_maintenance",
    description="Compaction et expiration des snapshots Iceberg",
    schedule="0 3 * * 0",  # dimanche 03:00 UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 0},
    params={"snapshot_retention_days": Param(7, type="integer", minimum=1)},
    tags=["iceberg", "maintenance", "spark"],
    doc_md=__doc__,
)
def iceberg_maintenance() -> None:
    @task(pool="spark", execution_timeout=timedelta(hours=1))
    def maintain() -> dict[str, Any]:
        from taxi_pipeline.jobs import run_maintenance

        retention = int(get_current_context()["params"]["snapshot_retention_days"])
        return run_maintenance(retention)

    maintain()


iceberg_maintenance()
