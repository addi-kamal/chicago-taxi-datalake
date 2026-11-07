"""DDL des tables Iceberg (source de vérité du modèle de données).

Les tables sont créées explicitement (``CREATE TABLE IF NOT EXISTS``) plutôt que
déduites des DataFrames : types, partitionnement, emplacement S3 et propriétés
sont ainsi maîtrisés et revus comme du code.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession

from taxi_pipeline.config import Layer, Settings
from taxi_pipeline.schemas import SOURCE_COLUMNS

TABLE_PROPERTIES: dict[str, str] = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": str(128 * 1024 * 1024),
    "write.distribution-mode": "hash",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "20",
}


@dataclass(frozen=True)
class TableSpec:
    layer: Layer
    name: str
    columns: str  # liste "nom TYPE" séparée par des virgules
    partitioned_by: str | None
    comment: str


_SOURCE_STRINGS = ",\n    ".join(f"{c} STRING" for c in SOURCE_COLUMNS)

BRONZE_TRIPS = TableSpec(
    layer="bronze",
    name="taxi_trips_raw",
    columns=f"""
    {_SOURCE_STRINGS},
    _source_file STRING,
    _ingested_at TIMESTAMP,
    _run_id STRING,
    trip_date DATE""",
    partitioned_by="trip_date",
    comment="Trajets bruts SODA, toutes colonnes en string, non modifiés",
)

SILVER_TRIPS = TableSpec(
    layer="silver",
    name="taxi_trips",
    columns="""
    trip_id STRING,
    taxi_id STRING,
    trip_start_ts TIMESTAMP_NTZ,
    trip_end_ts TIMESTAMP_NTZ,
    trip_date DATE,
    trip_seconds INT,
    trip_miles DOUBLE,
    pickup_census_tract STRING,
    dropoff_census_tract STRING,
    pickup_community_area INT,
    dropoff_community_area INT,
    fare DECIMAL(10,2),
    tips DECIMAL(10,2),
    tolls DECIMAL(10,2),
    extras DECIMAL(10,2),
    trip_total DECIMAL(10,2),
    payment_type STRING,
    company STRING,
    pickup_lat DOUBLE,
    pickup_lon DOUBLE,
    dropoff_lat DOUBLE,
    dropoff_lon DOUBLE,
    is_duration_outlier BOOLEAN,
    is_distance_outlier BOOLEAN,
    is_amount_inconsistent BOOLEAN,
    _ingested_at TIMESTAMP,
    _run_id STRING""",
    partitioned_by="days(trip_start_ts)",
    comment="Trajets typés, dédoublonnés et contrôlés (heure locale de Chicago, sans fuseau)",
)

SILVER_REJECTED = TableSpec(
    layer="silver",
    name="taxi_trips_rejected",
    columns=f"""
    {_SOURCE_STRINGS},
    trip_date DATE,
    reject_reason STRING,
    _source_file STRING,
    _ingested_at TIMESTAMP,
    _run_id STRING""",
    partitioned_by="trip_date",
    comment="Quarantaine : lignes violant une règle qualité bloquante",
)

SILVER_DQ_RESULTS = TableSpec(
    layer="silver",
    name="dq_results",
    columns="""
    run_id STRING,
    period_start DATE,
    period_end DATE,
    rule STRING,
    severity STRING,
    checked_rows BIGINT,
    failed_rows BIGINT,
    failed_pct DOUBLE,
    checked_at TIMESTAMP""",
    partitioned_by=None,
    comment="Résultats des contrôles qualité par exécution",
)

SILVER_DIM_COMMUNITY_AREA = TableSpec(
    layer="silver",
    name="dim_community_area",
    columns="""
    community_area_id INT,
    name STRING,
    side STRING""",
    partitioned_by=None,
    comment="Référentiel des 77 community areas de Chicago",
)

GOLD_DAILY_REVENUE = TableSpec(
    layer="gold",
    name="daily_revenue",
    columns="""
    trip_date DATE,
    trip_month DATE,
    trips BIGINT,
    total_revenue DECIMAL(18,2),
    fare_revenue DECIMAL(18,2),
    tips_revenue DECIMAL(18,2),
    tolls_revenue DECIMAL(18,2),
    extras_revenue DECIMAL(18,2),
    avg_ticket DECIMAL(10,2),
    tip_rate DOUBLE,
    active_taxis BIGINT""",
    partitioned_by="trip_month",
    comment="KPIs financiers quotidiens",
)

GOLD_TRIP_DURATION = TableSpec(
    layer="gold",
    name="trip_duration_stats",
    columns="""
    trip_date DATE,
    trip_month DATE,
    trip_hour INT,
    day_of_week INT,
    day_label STRING,
    trips BIGINT,
    avg_duration_min DOUBLE,
    median_duration_min DOUBLE,
    p90_duration_min DOUBLE,
    avg_distance_miles DOUBLE,
    avg_speed_mph DOUBLE""",
    partitioned_by="trip_month",
    comment="Durées / distances / vitesses par jour et heure de prise en charge (hors outliers)",
)

GOLD_TOP_PICKUP = TableSpec(
    layer="gold",
    name="top_pickup_areas",
    columns="""
    trip_month DATE,
    community_area_id INT,
    community_area_name STRING,
    side STRING,
    trips BIGINT,
    total_revenue DECIMAL(18,2),
    avg_ticket DECIMAL(10,2),
    share_of_trips DOUBLE,
    area_rank INT""",
    partitioned_by="trip_month",
    comment="Classement mensuel des zones de prise en charge",
)

GOLD_PAYMENT_MIX = TableSpec(
    layer="gold",
    name="payment_mix",
    columns="""
    trip_date DATE,
    trip_month DATE,
    payment_type STRING,
    trips BIGINT,
    total_revenue DECIMAL(18,2),
    share_of_trips DOUBLE,
    share_of_revenue DOUBLE""",
    partitioned_by="trip_month",
    comment="Répartition quotidienne par moyen de paiement",
)

GOLD_COMPANY_PERF = TableSpec(
    layer="gold",
    name="company_performance",
    columns="""
    trip_month DATE,
    company STRING,
    trips BIGINT,
    active_taxis BIGINT,
    total_revenue DECIMAL(18,2),
    avg_ticket DECIMAL(10,2),
    tip_rate DOUBLE,
    avg_trip_miles DOUBLE,
    revenue_rank INT""",
    partitioned_by="trip_month",
    comment="Performance mensuelle des compagnies",
)

GOLD_TABLES: tuple[TableSpec, ...] = (
    GOLD_DAILY_REVENUE,
    GOLD_TRIP_DURATION,
    GOLD_TOP_PICKUP,
    GOLD_PAYMENT_MIX,
    GOLD_COMPANY_PERF,
)

ALL_TABLES: tuple[TableSpec, ...] = (
    BRONZE_TRIPS,
    SILVER_TRIPS,
    SILVER_REJECTED,
    SILVER_DQ_RESULTS,
    SILVER_DIM_COMMUNITY_AREA,
    *GOLD_TABLES,
)


def create_table_ddl(settings: Settings, spec: TableSpec) -> str:
    props = ",\n    ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items())
    partition = f"\nPARTITIONED BY ({spec.partitioned_by})" if spec.partitioned_by else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {settings.table(spec.layer, spec.name)} ({spec.columns}\n)\n"
        f"USING iceberg{partition}\n"
        f"LOCATION '{settings.table_location(spec.layer, spec.name)}'\n"
        f"COMMENT '{spec.comment}'\n"
        f"TBLPROPERTIES (\n    {props}\n)"
    )


def ensure_namespace(spark: SparkSession, settings: Settings, layer: Layer) -> None:
    namespace = f"{settings.iceberg_catalog_name}.{settings.namespace(layer)}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def ensure_table(spark: SparkSession, settings: Settings, spec: TableSpec) -> str:
    """Crée namespace + table si besoin ; retourne l'identifiant complet."""
    ensure_namespace(spark, settings, spec.layer)
    spark.sql(create_table_ddl(settings, spec))
    return settings.table(spec.layer, spec.name)


def table_columns(spark: SparkSession, settings: Settings, spec: TableSpec) -> list[str]:
    return spark.table(settings.table(spec.layer, spec.name)).columns
