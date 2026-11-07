"""Les DAGs se chargent sans erreur et respectent les conventions (exécuté dans l'image Airflow)."""

from __future__ import annotations

from itertools import pairwise
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"


@pytest.fixture(scope="module")
def dagbag():  # type: ignore[no-untyped-def]
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.import_errors == {}


def test_e2e_dag_runs_automatically_and_in_order(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("chicago_taxi_e2e")
    assert dag is not None
    assert dag.is_paused_upon_creation is False
    assert dag.max_active_runs == 1
    order = ["load_bronze", "load_reference_data", "transform_silver", "quality_gate", "build_gold",
             "validate_serving_layer", "publish_dashboard"]  # fmt: skip
    for upstream, downstream in pairwise(order):
        assert upstream in dag.get_task(downstream).upstream_task_ids, (upstream, downstream)


def test_spark_tasks_use_single_slot_pool(dagbag) -> None:  # type: ignore[no-untyped-def]
    dag = dagbag.get_dag("chicago_taxi_e2e")
    for task_id in ["load_bronze", "load_reference_data", "transform_silver", "build_gold"]:
        assert dag.get_task(task_id).pool == "spark"


def test_maintenance_dag_paused_by_default(dagbag) -> None:  # type: ignore[no-untyped-def]
    assert dagbag.get_dag("iceberg_maintenance").is_paused_upon_creation is True
