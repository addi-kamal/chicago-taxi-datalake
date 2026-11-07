"""Schémas et contrats de colonnes des couches bronze / silver."""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType

# Colonnes source, dans l'ordre exact imposé à l'API SODA via `$select`.
SOURCE_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "taxi_id",
    "trip_start_timestamp",
    "trip_end_timestamp",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_centroid_latitude",
    "pickup_centroid_longitude",
    "pickup_centroid_location",
    "dropoff_centroid_latitude",
    "dropoff_centroid_longitude",
    "dropoff_centroid_location",
)

# Lecture de la landing : tout en string, jamais d'inférence de schéma.
LANDING_SCHEMA = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS])

BRONZE_TECH_COLUMNS: tuple[str, ...] = ("_source_file", "_ingested_at", "_run_id", "trip_date")

AMOUNT_COLUMNS: tuple[str, ...] = ("fare", "tips", "tolls", "extras", "trip_total")

# Ordre des colonnes de la table silver (doit correspondre au DDL de tables.py).
SILVER_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "taxi_id",
    "trip_start_ts",
    "trip_end_ts",
    "trip_date",
    "trip_seconds",
    "trip_miles",
    "pickup_census_tract",
    "dropoff_census_tract",
    "pickup_community_area",
    "dropoff_community_area",
    "fare",
    "tips",
    "tolls",
    "extras",
    "trip_total",
    "payment_type",
    "company",
    "pickup_lat",
    "pickup_lon",
    "dropoff_lat",
    "dropoff_lon",
    "is_duration_outlier",
    "is_distance_outlier",
    "is_amount_inconsistent",
    "_ingested_at",
    "_run_id",
)

REJECTED_COLUMNS: tuple[str, ...] = (
    *SOURCE_COLUMNS,
    "trip_date",
    "reject_reason",
    "_source_file",
    "_ingested_at",
    "_run_id",
)
