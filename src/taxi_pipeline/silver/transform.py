"""Transformations bronze -> silver (fonctions pures ``DataFrame -> DataFrame``).

Aucune I/O ici : tout est testable avec une SparkSession locale sans S3 ni catalogue.

Points d'attention :
- les timestamps source sont en **heure locale de Chicago sans fuseau** : ils sont
  typés en ``TIMESTAMP_NTZ`` et jamais convertis en UTC ;
- les timestamps source sont arrondis au quart d'heure : la durée fiable est
  ``trip_seconds``, pas ``trip_end_ts - trip_start_ts`` ;
- le typage se fait en **projections successives de colonnes plates** (préfixes
  ``_p_`` = valeur parsée, ``_t_`` = valeur typée et nettoyée). Les règles qualité
  référencent ces colonnes : l'optimiseur Catalyst ne réinjecte pas les expressions
  de parsing dans chaque règle (sinon l'arbre d'expressions explose -> OOM driver).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from taxi_pipeline.schemas import (
    AMOUNT_COLUMNS,
    REJECTED_COLUMNS,
    SILVER_COLUMNS,
    SOURCE_COLUMNS,
)
from taxi_pipeline.silver.quality import (
    CHICAGO_BBOX,
    TYPED_PREFIX,
    amount_inconsistent,
    distance_outlier,
    duration_outlier,
    reject_reasons,
    typed,
)

PARSED_PREFIX = "_p_"

TIMESTAMP_FORMATS: tuple[str, ...] = (
    "yyyy-MM-dd'T'HH:mm:ss.SSS",  # format API SODA
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",  # format des exports CSV du portail
)

STRING_COLUMNS: tuple[str, ...] = (
    "trip_id",
    "taxi_id",
    "pickup_census_tract",
    "dropoff_census_tract",
    "payment_type",
    "company",
)


# --- Helpers de typage ------------------------------------------------------------


def clean_str(name: str) -> Column:
    """Trim + chaîne vide -> null."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def to_ts_ntz(name: str) -> Column:
    value = F.trim(F.col(name))
    return F.coalesce(*[F.to_timestamp_ntz(value, F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def to_number(name: str, spark_type: str) -> Column:
    """Supprime '$', ',' et espaces puis caste (null si illisible)."""
    return F.regexp_replace(F.col(name), r"[$,\s]", "").cast(spark_type)


def _p(name: str) -> Column:
    return F.col(PARSED_PREFIX + name)


def _in_bbox(lat: Column, lon: Column) -> Column:
    b = CHICAGO_BBOX
    return lat.between(b["lat_min"], b["lat_max"]) & lon.between(b["lon_min"], b["lon_max"])


# --- Étapes ---------------------------------------------------------------------------


def parse_columns(raw: DataFrame) -> DataFrame:
    """Étape 1 : valeurs parsées (``_p_*``), sans règle métier. Colonnes brutes conservées."""
    parsed = [
        *[clean_str(c).alias(PARSED_PREFIX + c) for c in STRING_COLUMNS],
        to_ts_ntz("trip_start_timestamp").alias(PARSED_PREFIX + "trip_start_ts"),
        to_ts_ntz("trip_end_timestamp").alias(PARSED_PREFIX + "trip_end_ts"),
        to_number("trip_seconds", "double").cast("int").alias(PARSED_PREFIX + "trip_seconds"),
        to_number("trip_miles", "double").alias(PARSED_PREFIX + "trip_miles"),
        *[
            to_number(f"{side}_community_area", "double")
            .cast("int")
            .alias(f"{PARSED_PREFIX}{side}_community_area")
            for side in ("pickup", "dropoff")
        ],
        *[to_number(c, "decimal(10,2)").alias(PARSED_PREFIX + c) for c in AMOUNT_COLUMNS],
        *[
            to_number(f"{side}_centroid_{axis}itude", "double").alias(
                f"{PARSED_PREFIX}{side}_{axis[:3]}"
            )
            for side in ("pickup", "dropoff")
            for axis in ("lat", "long")
        ],
    ]
    return raw.select("*", *parsed)


def clean_columns(parsed: DataFrame) -> DataFrame:
    """Étape 2 : règles de nettoyage -> colonnes typées finales (``_t_*``)."""
    t = TYPED_PREFIX
    pickup_ok = _in_bbox(_p("pickup_lat"), _p("pickup_lon"))
    dropoff_ok = _in_bbox(_p("dropoff_lat"), _p("dropoff_lon"))

    def area(side: str) -> Column:
        value = _p(f"{side}_community_area")
        return F.when(value.between(1, 77), value)

    def area_nullified(side: str) -> Column:
        value = _p(f"{side}_community_area")
        return value.isNotNull() & ~value.between(1, 77)

    def coords_nullified(side: str, ok: Column) -> Column:
        return _p(f"{side}_lat").isNotNull() & _p(f"{side}_lon").isNotNull() & ~ok

    passthrough = [
        *STRING_COLUMNS,
        "trip_start_ts",
        "trip_end_ts",
        "trip_seconds",
        "trip_miles",
        *AMOUNT_COLUMNS,
    ]
    return parsed.select(
        *[c for c in parsed.columns if not c.startswith(PARSED_PREFIX)],
        *[_p(c).alias(t + c) for c in passthrough],
        F.to_date(_p("trip_start_ts")).alias(t + "trip_date"),
        area("pickup").alias(t + "pickup_community_area"),
        area("dropoff").alias(t + "dropoff_community_area"),
        F.when(pickup_ok, _p("pickup_lat")).alias(t + "pickup_lat"),
        F.when(pickup_ok, _p("pickup_lon")).alias(t + "pickup_lon"),
        F.when(dropoff_ok, _p("dropoff_lat")).alias(t + "dropoff_lat"),
        F.when(dropoff_ok, _p("dropoff_lon")).alias(t + "dropoff_lon"),
        F.coalesce(area_nullified("pickup") | area_nullified("dropoff"), F.lit(False)).alias(
            "_ca_nullified"
        ),
        F.coalesce(
            coords_nullified("pickup", pickup_ok) | coords_nullified("dropoff", dropoff_ok),
            F.lit(False),
        ).alias("_coords_nullified"),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Garde la version la plus récemment ingérée de chaque ``trip_id``.

    Départage final par hash du contenu brut : deux lignes de même ``trip_id`` issues du
    même fichier (même ``_ingested_at``, même ``_source_file``) donnent un résultat stable,
    quel que soit l'ordre de lecture.

    Les lignes sans ``trip_id`` ne sont pas dédoublonnées (elles seront rejetées
    individuellement par la règle ``missing_trip_id``).
    """
    trip_id = typed("trip_id")
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in SOURCE_COLUMNS])), 256)
    window = Window.partitionBy(trip_id).orderBy(
        F.col("_ingested_at").desc(),
        F.col("_source_file").desc(),
        row_hash.asc(),
    )
    with_id = (
        df.where(trip_id.isNotNull())
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )
    return with_id.unionByName(df.where(trip_id.isNull()))


def annotate(raw: DataFrame, period_start: date, period_end: date) -> DataFrame:
    """Typage + nettoyage + dédoublonnage + règles bloquantes (colonne ``reject_reasons``)."""
    return deduplicate(clean_columns(parse_columns(raw))).withColumn(
        "reject_reasons", reject_reasons(period_start, period_end)
    )


def valid_trips(annotated: DataFrame) -> DataFrame:
    """Lignes valides, au schéma de ``silver.taxi_trips`` (avec flags qualité)."""
    flat = annotated.where(F.size("reject_reasons") == 0).select(
        *[
            (F.col(c) if c.startswith("_") else typed(c)).alias(c)
            for c in SILVER_COLUMNS
            if not c.startswith("is_")
        ]
    )
    return flat.select(
        "*",
        duration_outlier().alias("is_duration_outlier"),
        distance_outlier().alias("is_distance_outlier"),
        amount_inconsistent().alias("is_amount_inconsistent"),
    ).select(*SILVER_COLUMNS)


def rejected_trips(annotated: DataFrame) -> DataFrame:
    """Lignes en quarantaine : valeurs brutes + raison(s) du rejet."""
    return (
        annotated.where(F.size("reject_reasons") > 0)
        .select(
            *[F.col(c) for c in REJECTED_COLUMNS if c != "reject_reason"],
            F.concat_ws(";", "reject_reasons").alias("reject_reason"),
        )
        .select(*REJECTED_COLUMNS)
    )


@dataclass(frozen=True)
class SilverFrames:
    annotated: DataFrame
    valid: DataFrame
    rejected: DataFrame


def build_silver(raw: DataFrame, period_start: date, period_end: date) -> SilverFrames:
    annotated = annotate(raw, period_start, period_end)
    return SilverFrames(annotated, valid_trips(annotated), rejected_trips(annotated))
