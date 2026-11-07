"""Tests d'intégration : exécutés DANS la stack Compose (`make test-integration`).

Ils utilisent des namespaces dédiés (préfixe ``it_``) et une landing dédiée :
aucune interférence avec les données du pipeline principal.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import pytest

from taxi_pipeline.config import Settings
from tests.conftest import SAMPLE_CSV, SAMPLE_END, SAMPLE_START


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    if os.environ.get("RUN_INTEGRATION") == "1":
        return
    skip = pytest.mark.skip(reason="RUN_INTEGRATION=1 requis (stack Docker Compose)")
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def it_settings() -> Settings:
    return Settings(
        iceberg_namespace_prefix="it_",
        landing_prefix="it_landing/chicago_taxi",
        pipeline_source="sample",
        pipeline_start_date=SAMPLE_START,
        pipeline_end_date=SAMPLE_END,
        sample_data_path=str(SAMPLE_CSV),
        spark_ui_enabled=False,
        spark_driver_memory="1g",
    )


@pytest.fixture(scope="session")
def it_spark(it_settings: Settings) -> Iterator[Any]:
    from taxi_pipeline.spark import spark_session

    with spark_session(it_settings, "integration-tests") as session:
        yield session
        # Nettoyage : suppression des tables de test (données + métadonnées).
        for layer in ("bronze", "silver", "gold"):
            ns = f"{it_settings.iceberg_catalog_name}.{it_settings.namespace(layer)}"  # type: ignore[arg-type]
            if session.catalog.databaseExists(ns):
                for t in session.sql(f"SHOW TABLES IN {ns}").collect():
                    session.sql(f"DROP TABLE IF EXISTS {ns}.{t['tableName']} PURGE")
                session.sql(f"DROP NAMESPACE IF EXISTS {ns}")
