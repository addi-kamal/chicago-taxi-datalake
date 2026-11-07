"""Configuration centralisée, lue exclusivement depuis l'environnement.

Toute différence entre le POC (MinIO) et la production (Ceph RGW, catalogue de
prod, cluster Spark) passe par ces variables : aucun code métier ne change.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Layer = Literal["bronze", "silver", "gold"]


class Settings(BaseSettings):
    """Paramètres du pipeline (noms de variables = noms des champs en majuscules).

    Les secrets n'ont pas de valeur par défaut exploitable : ils sont fournis par
    l'environnement (`.env` généré par ``scripts/generate_env.py``).
    """

    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False, frozen=True)

    # --- Stockage S3 ---------------------------------------------------------
    s3_endpoint: str = "http://minio:9000"
    s3_access_key: str = "datalake"
    s3_secret_key: str = ""
    s3_region: str = "us-east-1"
    s3_path_style: bool = True
    s3_use_ssl: bool = False
    bucket_bronze: str = "bronze"
    bucket_silver: str = "silver"
    bucket_gold: str = "gold"
    landing_prefix: str = "landing/chicago_taxi"

    # --- Catalogue Iceberg -----------------------------------------------------
    iceberg_catalog_name: str = "iceberg"
    iceberg_catalog_uri: str = "jdbc:postgresql://postgres:5432/iceberg_catalog"
    iceberg_catalog_user: str = "iceberg"
    iceberg_catalog_password: str = ""
    iceberg_warehouse: str = "s3://bronze/warehouse"
    # Préfixe des namespaces (ex. "it_" pour isoler les tests d'intégration).
    iceberg_namespace_prefix: str = ""

    # --- Spark -----------------------------------------------------------------
    spark_master: str = "local[*]"
    spark_driver_memory: str = "3g"
    spark_shuffle_partitions: int = 16
    spark_jars_dir: str = "/opt/spark-jars"
    spark_ui_enabled: bool = True

    # --- Source SODA -------------------------------------------------------------
    soda_base_url: str = "https://data.cityofchicago.org/resource"
    soda_dataset_id: str = "wrvz-psew"
    soda_app_token: str | None = None
    soda_page_size: int = Field(default=50_000, gt=0, le=50_000)
    soda_timeout_s: int = 180

    # --- Pipeline ----------------------------------------------------------------
    pipeline_source: Literal["soda", "sample"] = "soda"
    pipeline_start_date: date = date(2023, 1, 1)
    pipeline_end_date: date = date(2023, 1, 8)
    sample_data_path: str = "/opt/airflow/project/tests/fixtures/sample_trips.csv"
    seeds_dir: str = "/opt/airflow/project/seeds"
    dq_max_reject_pct: float = Field(default=5.0, ge=0, le=100)

    # --- Trino ---------------------------------------------------------------------
    trino_host: str = "trino"
    trino_port: int = 8080
    trino_user: str = "pipeline"

    # --- Superset ------------------------------------------------------------------
    superset_url: str = "http://superset:8088"
    superset_admin_user: str = "superset_admin"
    superset_admin_password: str = ""
    superset_public_url: str = "http://localhost:8088"

    @model_validator(mode="after")
    def _check_period(self) -> Settings:
        if self.pipeline_end_date <= self.pipeline_start_date:
            raise ValueError("PIPELINE_END_DATE doit être strictement après PIPELINE_START_DATE")
        return self

    # --- Helpers -------------------------------------------------------------------
    def bucket(self, layer: Layer) -> str:
        return {
            "bronze": self.bucket_bronze,
            "silver": self.bucket_silver,
            "gold": self.bucket_gold,
        }[layer]

    def namespace(self, layer: Layer) -> str:
        """Namespace Iceberg d'une couche, sans le catalogue (ex. ``silver``)."""
        return f"{self.iceberg_namespace_prefix}{layer}"

    def table(self, layer: Layer, name: str) -> str:
        """Identifiant complet ``catalogue.namespace.table`` (identique Spark / Trino)."""
        return f"{self.iceberg_catalog_name}.{self.namespace(layer)}.{name}"

    def table_location(self, layer: Layer, name: str) -> str:
        """Emplacement S3 d'une table : un bucket par couche."""
        return f"s3://{self.bucket(layer)}/warehouse/{self.namespace(layer)}/{name}"

    @property
    def all_buckets(self) -> list[str]:
        return [self.bucket_bronze, self.bucket_silver, self.bucket_gold]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
