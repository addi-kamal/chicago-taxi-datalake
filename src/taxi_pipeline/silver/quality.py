"""Règles qualité de la couche silver (expressions Spark pures).

Trois niveaux :
- **bloquant** : la ligne part en quarantaine (``taxi_trips_rejected``) ;
- **signalé** : la ligne est conservée avec un flag booléen ``is_*`` ;
- **nettoyage** : une valeur invalide est mise à null (zone, coordonnées).

Les résultats sont historisés dans ``silver.dq_results`` ; le pipeline échoue si
le taux de rejet bloquant dépasse ``DQ_MAX_REJECT_PCT`` ou si aucune ligne n'est à contrôler.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from taxi_pipeline.schemas import AMOUNT_COLUMNS

# 6 h : au-delà, presque toujours un compteur resté allumé (p99,9 observé ~4,7 h).
MAX_TRIP_SECONDS = 21_600
MAX_TRIP_MILES = 500.0
MAX_SPEED_MPH = 100.0
AMOUNT_TOLERANCE = 1.0
# Boîte englobante (large) de la ville de Chicago.
CHICAGO_BBOX = {"lat_min": 41.60, "lat_max": 42.05, "lon_min": -87.95, "lon_max": -87.50}


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # blocking | flag | cleaning | info
    description: str


# --- Règles bloquantes (sur les colonnes typées `_t_*`) ---------------------------

TYPED_PREFIX = "_t_"


def typed(name: str) -> Column:
    """Colonne typée/nettoyée produite par ``silver.transform.clean_columns``."""
    return F.col(TYPED_PREFIX + name)


def _blocking_conditions(period_start: date, period_end: date) -> list[tuple[Rule, Column]]:
    start = F.lit(period_start.isoformat()).cast("timestamp_ntz")
    end = F.lit(period_end.isoformat()).cast("timestamp_ntz")
    start_ts, end_ts = typed("trip_start_ts"), typed("trip_end_ts")
    negative = F.lit(False)
    for c in AMOUNT_COLUMNS:
        negative = negative | F.coalesce(typed(c) < 0, F.lit(False))
    return [
        (
            Rule("missing_trip_id", "blocking", "trip_id absent"),
            typed("trip_id").isNull(),
        ),
        (
            Rule(
                "missing_or_invalid_start_ts",
                "blocking",
                "trip_start_timestamp absent ou illisible",
            ),
            start_ts.isNull(),
        ),
        (
            Rule("start_ts_out_of_period", "blocking", "trip_start_ts hors de la période traitée"),
            start_ts.isNotNull() & ((start_ts < start) | (start_ts >= end)),
        ),
        (
            Rule("end_before_start", "blocking", "trip_end_ts antérieur à trip_start_ts"),
            F.coalesce(end_ts < start_ts, F.lit(False)),
        ),
        (
            Rule("negative_amount", "blocking", "montant négatif (fare/tips/tolls/extras/total)"),
            negative,
        ),
    ]


def reject_reasons(period_start: date, period_end: date) -> Column:
    """Tableau des règles bloquantes violées (vide si la ligne est valide)."""
    return F.array_compact(
        F.array(
            *[
                F.when(cond, F.lit(rule.name))
                for rule, cond in _blocking_conditions(period_start, period_end)
            ]
        )
    )


def blocking_rules(period_start: date, period_end: date) -> list[Rule]:
    return [rule for rule, _ in _blocking_conditions(period_start, period_end)]


# --- Flags (sur les colonnes silver) ----------------------------------------------


def duration_outlier(df_col: Callable[[str], Column] = F.col) -> Column:
    secs = df_col("trip_seconds")
    return F.coalesce(secs.isNull() | (secs <= 0) | (secs > MAX_TRIP_SECONDS), F.lit(True))


def distance_outlier(df_col: Callable[[str], Column] = F.col) -> Column:
    miles, secs = df_col("trip_miles"), df_col("trip_seconds")
    speed = F.when(secs > 0, miles / (secs / F.lit(3600.0)))
    return F.coalesce(
        (miles < 0) | (miles > MAX_TRIP_MILES) | F.coalesce(speed > MAX_SPEED_MPH, F.lit(False)),
        F.lit(False),
    )


def amount_inconsistent(df_col: Callable[[str], Column] = F.col) -> Column:
    parts = sum(
        (F.coalesce(df_col(c), F.lit(0)) for c in ("fare", "tips", "tolls", "extras")),
        F.lit(0),
    )
    total = df_col("trip_total")
    return F.coalesce(F.abs(total - parts) > AMOUNT_TOLERANCE, F.lit(True))


FLAG_RULES: tuple[Rule, ...] = (
    Rule("is_duration_outlier", "flag", f"trip_seconds null, <= 0 ou > {MAX_TRIP_SECONDS}"),
    Rule(
        "is_distance_outlier",
        "flag",
        f"trip_miles < 0 ou > {MAX_TRIP_MILES:g}, ou vitesse > {MAX_SPEED_MPH:g} mph",
    ),
    Rule("is_amount_inconsistent", "flag", "|trip_total - somme des composantes| > 1 $"),
)


# --- Résultats qualité ---------------------------------------------------------------


@dataclass(frozen=True)
class GateResult:
    passed: bool
    reject_pct: float
    threshold_pct: float
    checked_rows: int
    rejected_rows: int

    def message(self) -> str:
        if self.checked_rows == 0:
            return "Contrôle qualité ÉCHEC : aucune ligne à contrôler (source vide ?)"
        status = "OK" if self.passed else "ÉCHEC"
        return (
            f"Contrôle qualité {status} : {self.rejected_rows}/{self.checked_rows} lignes rejetées "
            f"({self.reject_pct:.3f} %) - seuil {self.threshold_pct:.2f} %"
        )


def _pct(failed: int, checked: int) -> float:
    return round(100.0 * failed / checked, 4) if checked else 0.0


def compute_dq_results(
    annotated: DataFrame,
    valid: DataFrame,
    *,
    input_rows: int,
    duplicates_removed: int,
    run_id: str,
    period_start: date,
    period_end: date,
    checked_at: datetime,
) -> list[dict[str, Any]]:
    """Calcule toutes les métriques qualité en deux agrégations (annotated + valid)."""
    rules = blocking_rules(period_start, period_end)
    agg_annotated = annotated.agg(
        F.count(F.lit(1)).alias("checked"),
        F.sum(F.when(F.size("reject_reasons") > 0, 1).otherwise(0)).alias("rejected"),
        *[
            F.sum(F.when(F.array_contains("reject_reasons", r.name), 1).otherwise(0)).alias(r.name)
            for r in rules
        ],
        F.sum(F.col("_ca_nullified").cast("int")).alias("community_area_nullified"),
        F.sum(F.col("_coords_nullified").cast("int")).alias("coordinates_nullified"),
    ).first()
    agg_valid = valid.agg(
        F.count(F.lit(1)).alias("valid"),
        *[F.sum(F.col(r.name).cast("int")).alias(r.name) for r in FLAG_RULES],
    ).first()
    assert agg_annotated is not None and agg_valid is not None

    checked = int(agg_annotated["checked"] or 0)
    valid_rows = int(agg_valid["valid"] or 0)
    base = {"run_id": run_id, "period_start": period_start, "period_end": period_end}

    def row(rule: str, severity: str, checked_rows: int, failed: int) -> dict[str, Any]:
        return {
            **base,
            "rule": rule,
            "severity": severity,
            "checked_rows": checked_rows,
            "failed_rows": failed,
            "failed_pct": _pct(failed, checked_rows),
            "checked_at": checked_at,
        }

    results = [
        row("duplicate_trip_id", "info", input_rows, duplicates_removed),
        *[row(r.name, r.severity, checked, int(agg_annotated[r.name] or 0)) for r in rules],
        row("total_rejected", "blocking", checked, int(agg_annotated["rejected"] or 0)),
        *[row(r.name, r.severity, valid_rows, int(agg_valid[r.name] or 0)) for r in FLAG_RULES],
        row(
            "community_area_nullified",
            "cleaning",
            checked,
            int(agg_annotated["community_area_nullified"] or 0),
        ),
        row(
            "coordinates_nullified",
            "cleaning",
            checked,
            int(agg_annotated["coordinates_nullified"] or 0),
        ),
    ]
    return results


def evaluate_gate(results: list[dict[str, Any]], max_reject_pct: float) -> GateResult:
    total = next(r for r in results if r["rule"] == "total_rejected")
    checked = int(total["checked_rows"])
    pct = float(total["failed_pct"])
    return GateResult(
        passed=checked > 0 and pct <= max_reject_pct,
        reject_pct=pct,
        threshold_pct=max_reject_pct,
        checked_rows=checked,
        rejected_rows=int(total["failed_rows"]),
    )
