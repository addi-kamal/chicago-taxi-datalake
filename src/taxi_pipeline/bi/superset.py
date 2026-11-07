"""Provisionnement « dashboard as code » de Superset via son API REST.

Deux dashboards, chacun pour son public :
- **Activité** (``chicago-taxi``) : KPIs métier lus dans la gold ;
- **Qualité des données** (``chicago-taxi-quality``) : volumétrie par couche, rejets,
  signalements et historique des contrôles, lus dans silver.

Exécuté en fin de pipeline (les datasets Superset lisent le schéma des tables
dans Trino : elles doivent exister). Idempotent :
- la connexion Trino et les datasets sont créés ou rafraîchis ;
- les dashboards et leurs graphiques sont recréés à l'identique à chaque exécution.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import requests

from taxi_pipeline.config import Settings

log = logging.getLogger(__name__)

DATABASE_NAME = "Datalake (Trino / Iceberg)"
DASHBOARD_SLUG = "chicago-taxi"
DASHBOARD_TITLE = "Chicago Taxi - Activité"
QUALITY_SLUG = "chicago-taxi-quality"
QUALITY_TITLE = "Chicago Taxi - Qualité des données"


class SupersetError(RuntimeError):
    pass


# --- Encodage rison (paramètre `q` de l'API Superset) ---------------------------------


def rison(value: Any) -> str:
    if value is None:
        return "!n"
    if value is True:
        return "!t"
    if value is False:
        return "!f"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        return "'" + value.replace("!", "!!").replace("'", "!'") + "'"
    if isinstance(value, list | tuple):
        return "!(" + ",".join(rison(v) for v in value) + ")"
    if isinstance(value, dict):
        return "(" + ",".join(f"{k}:{rison(v)}" for k, v in value.items()) + ")"
    raise TypeError(f"Type non supporté par rison : {type(value)}")


# --- Client ---------------------------------------------------------------------------------


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str, timeout_s: int = 120) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout_s = timeout_s
        self.session = requests.Session()

    def wait_ready(self, timeout_s: int = 600) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                if self.session.get(f"{self.base_url}/health", timeout=10).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(5)
        raise SupersetError(f"Superset indisponible après {timeout_s}s ({self.base_url})")

    def login(self) -> None:
        resp = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            },
            timeout=self.timeout_s,
        )
        self._check(resp, "login")
        self.session.headers["Authorization"] = f"Bearer {resp.json()['access_token']}"
        csrf = self.session.get(
            f"{self.base_url}/api/v1/security/csrf_token/", timeout=self.timeout_s
        )
        self._check(csrf, "csrf_token")
        self.session.headers["X-CSRFToken"] = csrf.json()["result"]
        self.session.headers["Referer"] = self.base_url

    @staticmethod
    def _check(resp: requests.Response, what: str) -> None:
        if not resp.ok:
            raise SupersetError(f"Superset {what} -> HTTP {resp.status_code} : {resp.text[:1000]}")

    def request(self, method: str, path: str, **kwargs: Any) -> Any:
        resp = self.session.request(
            method, f"{self.base_url}{path}", timeout=self.timeout_s, **kwargs
        )
        self._check(resp, f"{method} {path}")
        return resp.json() if resp.content else {}

    def list_all(self, resource: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 0
        while True:
            data = self.request(
                "GET", f"/api/v1/{resource}/", params={"q": rison({"page": page, "page_size": 100})}
            )
            batch = data.get("result", [])
            items.extend(batch)
            if len(batch) < 100:
                return items
            page += 1


# --- Définition déclarative du dashboard -------------------------------------------------------


def metric(sql: str, label: str) -> dict[str, Any]:
    return {
        "expressionType": "SQL",
        "sqlExpression": sql,
        "label": label,
        "hasCustomLabel": True,
        "optionName": "metric_" + "".join(ch for ch in label.lower() if ch.isalnum()),
    }


def no_time_filter(column: str) -> dict[str, Any]:
    return {
        "clause": "WHERE",
        "subject": column,
        "operator": "TEMPORAL_RANGE",
        "comparator": "No filter",
        "expressionType": "SIMPLE",
    }


TRIPS = metric("SUM(trips)", "Trajets")
REVENUE = metric("SUM(total_revenue)", "Chiffre d'affaires ($)")


@dataclass(frozen=True)
class DatasetDef:
    key: str
    schema: str  # couche : bronze | silver | gold
    table: str
    sql: str | None = None  # dataset virtuel si renseigné


@dataclass(frozen=True)
class ChartDef:
    key: str
    title: str
    dataset: str
    viz_type: str
    params: dict[str, Any]
    width: int
    height: int = 50
    description: str = ""


@dataclass
class Layout:
    rows: list[list[str]] = field(default_factory=list)  # clés de ChartDef ou "md:<texte>"


def dataset_defs(settings: Settings) -> list[DatasetDef]:
    dq = settings.table("silver", "dq_results")
    trips = settings.table("silver", "taxi_trips")
    return [
        DatasetDef("daily_revenue", "gold", "daily_revenue"),
        DatasetDef("trip_duration_stats", "gold", "trip_duration_stats"),
        DatasetDef("top_pickup_areas", "gold", "top_pickup_areas"),
        DatasetDef("payment_mix", "gold", "payment_mix"),
        DatasetDef("company_performance", "gold", "company_performance"),
        DatasetDef(
            "dq_latest",
            "silver",
            "dq_latest_run",
            sql=(
                "SELECT rule, severity, checked_rows, failed_rows, failed_pct, checked_at, run_id\n"
                f"FROM {dq}\n"
                f"WHERE run_id = (SELECT max_by(run_id, checked_at) FROM {dq})"
            ),
        ),
        DatasetDef(
            "layer_volumes",
            "silver",
            "layer_volumes",
            sql=(
                "SELECT '1. bronze (brut)' AS layer, count(*) AS row_count "
                f"FROM {settings.table('bronze', 'taxi_trips_raw')}\n"
                "UNION ALL SELECT '2. silver (valide)', count(*) "
                f"FROM {settings.table('silver', 'taxi_trips')}\n"
                "UNION ALL SELECT '3. silver (quarantaine)', count(*) "
                f"FROM {settings.table('silver', 'taxi_trips_rejected')}"
            ),
        ),
        DatasetDef("rejected", "silver", "taxi_trips_rejected"),
        DatasetDef(
            "dq_flags_daily",
            "silver",
            "dq_flags_daily",
            sql=(
                "SELECT trip_date, count(*) AS trips,\n"
                "       count_if(is_duration_outlier) AS duration_outliers,\n"
                "       count_if(is_distance_outlier) AS distance_outliers,\n"
                "       count_if(is_amount_inconsistent) AS amount_inconsistent\n"
                f"FROM {trips}\n"
                "GROUP BY trip_date"
            ),
        ),
        DatasetDef(
            "dq_history",
            "silver",
            "dq_history",
            sql=(
                "SELECT run_id, max(checked_at) AS checked_at,\n"
                "  max(period_start) AS period_start, max(period_end) AS period_end,\n"
                "  max(if(rule = 'total_rejected', checked_rows)) AS checked_rows,\n"
                "  max(if(rule = 'total_rejected', failed_rows)) AS rejected_rows,\n"
                "  max(if(rule = 'total_rejected', failed_pct)) AS reject_pct,\n"
                "  sum(if(severity = 'flag', failed_rows, 0)) AS flagged_values\n"
                f"FROM {dq}\n"
                "GROUP BY run_id"
            ),
        ),
    ]


def chart_defs() -> list[ChartDef]:
    big = {"header_font_size": 0.4, "subheader_font_size": 0.15, "adhoc_filters": []}
    return [
        ChartDef(
            "kpi_trips",
            "Trajets",
            "daily_revenue",
            "big_number_total",
            {**big, "metric": TRIPS, "y_axis_format": "SMART_NUMBER"},
            width=3,
            height=30,
        ),
        ChartDef(
            "kpi_revenue",
            "Chiffre d'affaires",
            "daily_revenue",
            "big_number_total",
            {**big, "metric": REVENUE, "y_axis_format": "$,.3s"},
            width=3,
            height=30,
        ),
        ChartDef(
            "kpi_ticket",
            "Ticket moyen",
            "daily_revenue",
            "big_number_total",
            {
                **big,
                "metric": metric(
                    "CAST(SUM(total_revenue) AS DOUBLE) / SUM(trips)", "Ticket moyen ($)"
                ),
                "y_axis_format": "$,.2f",
            },
            width=3,
            height=30,
        ),
        ChartDef(
            "kpi_tip_rate",
            "Taux de pourboire",
            "daily_revenue",
            "big_number_total",
            {
                **big,
                "metric": metric(
                    "CAST(SUM(tips_revenue) AS DOUBLE) / CAST(SUM(fare_revenue) AS DOUBLE)",
                    "Taux de pourboire",
                ),
                "y_axis_format": ".1%",
            },
            width=3,
            height=30,
        ),
        ChartDef(
            "revenue_daily",
            "Chiffre d'affaires quotidien",
            "daily_revenue",
            "echarts_timeseries_line",
            {
                "x_axis": "trip_date",
                "time_grain_sqla": "P1D",
                "metrics": [REVENUE],
                "groupby": [],
                "adhoc_filters": [no_time_filter("trip_date")],
                "row_limit": 10000,
                "y_axis_format": "$,.3s",
                "x_axis_time_format": "smart_date",
                "rich_tooltip": True,
                "show_legend": False,
                "zoomable": True,
                "markerEnabled": False,
                "area": True,
                "opacity": 0.2,
            },
            width=8,
        ),
        ChartDef(
            "payment_share",
            "Répartition par moyen de paiement",
            "payment_mix",
            "pie",
            {
                "groupby": ["payment_type"],
                "metric": TRIPS,
                "adhoc_filters": [],
                "row_limit": 10,
                "donut": True,
                "show_labels": True,
                "label_type": "key_percent",
                "labels_outside": True,
                "show_legend": True,
                "legendOrientation": "bottom",
                "number_format": "SMART_NUMBER",
                "sort_by_metric": True,
            },
            width=4,
        ),
        ChartDef(
            "trips_by_payment",
            "Trajets quotidiens par moyen de paiement",
            "payment_mix",
            "echarts_timeseries_bar",
            {
                "x_axis": "trip_date",
                "time_grain_sqla": "P1D",
                "metrics": [TRIPS],
                "groupby": ["payment_type"],
                "adhoc_filters": [no_time_filter("trip_date")],
                "row_limit": 10000,
                "stack": "Stack",
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
                "x_axis_time_format": "smart_date",
            },
            width=12,
        ),
        ChartDef(
            "top_areas",
            "Top 15 des zones de prise en charge",
            "top_pickup_areas",
            "echarts_timeseries_bar",
            {
                "x_axis": "community_area_name",
                "metrics": [TRIPS],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 15,
                "order_desc": True,
                "orientation": "horizontal",
                "x_axis_sort": "Trajets",
                "x_axis_sort_asc": True,
                "show_legend": False,
                "show_value": True,
                "y_axis_format": "SMART_NUMBER",
            },
            width=6,
            height=60,
        ),
        ChartDef(
            "hourly_heatmap",
            "Affluence par jour de semaine et heure",
            "trip_duration_stats",
            "heatmap_v2",
            {
                # Libellé texte : Superset affiche la valeur numérique 0 comme <NULL>.
                "x_axis": {
                    "expressionType": "SQL",
                    "sqlExpression": "lpad(CAST(trip_hour AS varchar), 2, '0') || 'h'",
                    "label": "Heure",
                },
                "groupby": "day_label",
                "metric": TRIPS,
                "adhoc_filters": [],
                "row_limit": 10000,
                "linear_color_scheme": "blue_white_yellow",
                "normalize_across": "heatmap",
                "sort_x_axis": "alpha_asc",
                "sort_y_axis": "alpha_asc",
                "show_legend": True,
                "show_values": False,
                "xscale_interval": -1,
                "yscale_interval": -1,
                "left_margin": "auto",
                "bottom_margin": "auto",
                "y_axis_format": "SMART_NUMBER",
            },
            width=6,
            height=60,
        ),
        ChartDef(
            "duration_by_hour",
            "Durée moyenne d'un trajet selon l'heure (min)",
            "trip_duration_stats",
            "echarts_timeseries_line",
            {
                "x_axis": "trip_hour",
                "metrics": [
                    metric("SUM(avg_duration_min * trips) / SUM(trips)", "Durée moyenne (min)"),
                    metric("SUM(p90_duration_min * trips) / SUM(trips)", "P90 durée (min)"),
                ],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
                "x_axis_sort": "trip_hour",
                "x_axis_sort_asc": True,
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "y_axis_format": ",.1f",
                "markerEnabled": True,
            },
            width=6,
        ),
        ChartDef(
            "company_table",
            "Compagnies : volume, CA, pourboires",
            "company_performance",
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["company"],
                "metrics": [
                    TRIPS,
                    REVENUE,
                    metric("MAX(active_taxis)", "Taxis actifs (max/mois)"),
                    metric("AVG(tip_rate)", "Taux de pourboire moyen"),
                ],
                "all_columns": [],
                "percent_metrics": [],
                "adhoc_filters": [],
                "timeseries_limit_metric": REVENUE,
                "order_desc": True,
                "row_limit": 15,
                "server_page_length": 10,
                "show_cell_bars": True,
                "color_pn": False,
                "column_config": {
                    "Taux de pourboire moyen": {"d3NumberFormat": ".1%"},
                    "Chiffre d'affaires ($)": {"d3NumberFormat": "$,.0f"},
                },
            },
            width=6,
        ),
    ]


def _layer_metric(prefix: str, label: str) -> dict[str, Any]:
    return metric(f"SUM(CASE WHEN layer LIKE '{prefix}%' THEN row_count END)", label)


def quality_chart_defs() -> list[ChartDef]:
    big = {"header_font_size": 0.4, "subheader_font_size": 0.15, "adhoc_filters": []}
    flag_rate = "CAST(SUM({col}) AS DOUBLE) / SUM(trips)"
    return [
        ChartDef(
            "q_kpi_bronze",
            "Lignes ingérées (bronze)",
            "layer_volumes",
            "big_number_total",
            {**big, "metric": _layer_metric("1.", "Lignes bronze"), "y_axis_format": ",d"},
            width=3,
            height=30,
        ),
        ChartDef(
            "q_kpi_silver",
            "Trajets valides (silver)",
            "layer_volumes",
            "big_number_total",
            {**big, "metric": _layer_metric("2.", "Lignes silver"), "y_axis_format": ",d"},
            width=3,
            height=30,
        ),
        ChartDef(
            "q_kpi_rejected",
            "Lignes en quarantaine",
            "layer_volumes",
            "big_number_total",
            {**big, "metric": _layer_metric("3.", "Lignes rejetées"), "y_axis_format": ",d"},
            width=3,
            height=30,
        ),
        ChartDef(
            "q_kpi_reject_rate",
            "Taux de rejet (dernier run)",
            "dq_latest",
            "big_number_total",
            {
                **big,
                "metric": metric(
                    "MAX(CASE WHEN rule = 'total_rejected' THEN failed_pct END) / 100",
                    "Taux de rejet",
                ),
                "y_axis_format": ".3%",
            },
            width=3,
            height=30,
            description="Le pipeline s'arrête avant gold si ce taux dépasse DQ_MAX_REJECT_PCT.",
        ),
        ChartDef(
            "q_layer_volumes",
            "Volumétrie par couche",
            "layer_volumes",
            "echarts_timeseries_bar",
            {
                "x_axis": "layer",
                "metrics": [metric("SUM(row_count)", "Lignes")],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 10,
                "x_axis_sort": "layer",
                "x_axis_sort_asc": True,
                "show_legend": False,
                "show_value": True,
                "y_axis_format": "SMART_NUMBER",
            },
            width=5,
        ),
        ChartDef(
            "q_failures_by_rule",
            "Lignes en échec par règle (dernier run)",
            "dq_latest",
            "echarts_timeseries_bar",
            {
                "x_axis": "rule",
                "metrics": [metric("SUM(failed_rows)", "Lignes en échec")],
                "groupby": ["severity"],
                "adhoc_filters": [
                    {
                        "clause": "WHERE",
                        "expressionType": "SQL",
                        "sqlExpression": "rule <> 'total_rejected'",
                    }
                ],
                "row_limit": 50,
                "orientation": "horizontal",
                "stack": "Stack",
                "show_legend": True,
                "legendOrientation": "top",
                "show_value": True,
                "y_axis_format": "SMART_NUMBER",
            },
            width=7,
            description="bloquant : quarantaine ; flag : ligne conservée et signalée ; "
            "cleaning : valeur mise à null ; info : doublons supprimés.",
        ),
        ChartDef(
            "q_rejects_daily",
            "Lignes rejetées par jour et par motif",
            "rejected",
            "echarts_timeseries_bar",
            {
                "x_axis": "trip_date",
                "time_grain_sqla": "P1D",
                "metrics": [metric("COUNT(*)", "Lignes rejetées")],
                "groupby": ["reject_reason"],
                "adhoc_filters": [no_time_filter("trip_date")],
                "row_limit": 10000,
                "stack": "Stack",
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "y_axis_format": "SMART_NUMBER",
                "x_axis_time_format": "smart_date",
            },
            width=6,
        ),
        ChartDef(
            "q_flags_daily",
            "Part des trajets signalés par jour",
            "dq_flags_daily",
            "echarts_timeseries_line",
            {
                "x_axis": "trip_date",
                "time_grain_sqla": "P1D",
                "metrics": [
                    metric(flag_rate.format(col="duration_outliers"), "Durée aberrante"),
                    metric(flag_rate.format(col="distance_outliers"), "Distance aberrante"),
                    metric(flag_rate.format(col="amount_inconsistent"), "Montant incohérent"),
                ],
                "groupby": [],
                "adhoc_filters": [no_time_filter("trip_date")],
                "row_limit": 10000,
                "show_legend": True,
                "legendOrientation": "top",
                "rich_tooltip": True,
                "y_axis_format": ".1%",
                "x_axis_time_format": "smart_date",
                "markerEnabled": False,
            },
            width=6,
            description="Lignes conservées en silver mais signalées (flags is_*).",
        ),
        ChartDef(
            "q_dq_latest",
            "Contrôles qualité - détail du dernier run",
            "dq_latest",
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "rule",
                    "severity",
                    "checked_rows",
                    "failed_rows",
                    "failed_pct",
                    "checked_at",
                ],
                "order_by_cols": ['["severity", true]', '["rule", true]'],
                "adhoc_filters": [],
                "row_limit": 100,
                "server_page_length": 20,
                "show_cell_bars": True,
                "column_config": {
                    "failed_pct": {"d3NumberFormat": ",.4f"},
                    "checked_rows": {"d3NumberFormat": ",d"},
                    "failed_rows": {"d3NumberFormat": ",d"},
                },
            },
            width=12,
            height=58,
            description="failed_pct est exprimé en pourcentage.",
        ),
        ChartDef(
            "q_history",
            "Historique des exécutions",
            "dq_history",
            "table",
            {
                "query_mode": "raw",
                "all_columns": [
                    "checked_at",
                    "run_id",
                    "period_start",
                    "period_end",
                    "checked_rows",
                    "rejected_rows",
                    "reject_pct",
                    "flagged_values",
                ],
                "order_by_cols": ['["checked_at", false]'],
                "adhoc_filters": [],
                "row_limit": 100,
                "server_page_length": 10,
                "column_config": {
                    "reject_pct": {"d3NumberFormat": ",.4f"},
                    "checked_rows": {"d3NumberFormat": ",d"},
                    "rejected_rows": {"d3NumberFormat": ",d"},
                    "flagged_values": {"d3NumberFormat": ",d"},
                },
            },
            width=12,
            height=28,
            description="Un run par ligne : permet de suivre la qualité dans le temps.",
        ),
    ]


Period = tuple[str, str]  # premier et dernier jour présents en gold (inclus)


def default_period(settings: Settings) -> Period:
    last = settings.pipeline_end_date - timedelta(days=1)
    return settings.pipeline_start_date.isoformat(), last.isoformat()


def loaded_period(settings: Settings) -> Period:
    """Période réellement chargée en gold (lue dans Trino), et non celle de la config.

    Un run déclenché avec une autre période (Trigger DAG w/ config) est ainsi affiché
    correctement dans l'en-tête des dashboards.
    """
    from taxi_pipeline.serving.trino_checks import query

    try:
        rows = query(
            settings,
            f"SELECT min(trip_date), max(trip_date) FROM {settings.table('gold', 'daily_revenue')}",
        )
        if rows and rows[0][0] is not None:
            return str(rows[0][0]), str(rows[0][1])
    except Exception as exc:  # l'en-tête ne doit jamais bloquer la publication
        log.warning("Période chargée illisible dans Trino (%s) : valeur de la config", exc)
    return default_period(settings)


def intro_markdown(settings: Settings, period: Period) -> str:
    return (
        "## Chicago Taxi Trips - activité\n"
        f"Données chargées : du **{period[0]}** au **{period[1]}** (inclus) · "
        "Source : API SODA `wrvz-psew` (City of Chicago) · "
        "Chaîne : MinIO/S3 → Iceberg (Spark) → **Trino** → Superset · Orchestration : Airflow.\n\n"
        "Montants en dollars US ; horaires en heure locale de Chicago. "
        "Définitions des KPIs : README, section Modèle de données. "
        "Fiabilité des chiffres : "
        f"[dashboard Qualité des données](/superset/dashboard/{QUALITY_SLUG}/)."
    )


def quality_intro_markdown(settings: Settings, period: Period) -> str:
    return (
        "## Chicago Taxi Trips - qualité des données\n"
        f"Données chargées : du **{period[0]}** au **{period[1]}** (inclus) · "
        f"Seuil du quality gate : **{settings.dq_max_reject_pct:g} %** de rejet.\n\n"
        "**Bloquant** : la ligne part en quarantaine (`silver.taxi_trips_rejected`) · "
        "**Signalé** : la ligne est conservée avec un flag `is_*` · "
        "**Nettoyage** : la valeur invalide est mise à null · "
        "**Info** : doublons de `trip_id` supprimés. "
        f"Indicateurs métier : [dashboard Activité](/superset/dashboard/{DASHBOARD_SLUG}/)."
    )


@dataclass(frozen=True)
class DashboardDef:
    slug: str
    title: str
    layout: list[list[str]]  # clés de ChartDef ou "md:<nom>"
    markdown: dict[str, tuple[str, int, int]]  # nom -> (texte, largeur, hauteur)


ACTIVITY_LAYOUT: list[list[str]] = [
    ["md:intro"],
    ["kpi_trips", "kpi_revenue", "kpi_ticket", "kpi_tip_rate"],
    ["revenue_daily", "payment_share"],
    ["trips_by_payment"],
    ["top_areas", "hourly_heatmap"],
    ["duration_by_hour", "company_table"],
]

QUALITY_LAYOUT: list[list[str]] = [
    ["md:intro"],
    ["q_kpi_bronze", "q_kpi_silver", "q_kpi_rejected", "q_kpi_reject_rate"],
    ["q_layer_volumes", "q_failures_by_rule"],
    ["q_rejects_daily", "q_flags_daily"],
    ["q_dq_latest"],
    ["q_history"],
]


def dashboard_defs(settings: Settings, period: Period | None = None) -> list[DashboardDef]:
    period = period or default_period(settings)
    return [
        DashboardDef(
            DASHBOARD_SLUG,
            DASHBOARD_TITLE,
            ACTIVITY_LAYOUT,
            {"intro": (intro_markdown(settings, period), 12, 22)},
        ),
        DashboardDef(
            QUALITY_SLUG,
            QUALITY_TITLE,
            QUALITY_LAYOUT,
            {"intro": (quality_intro_markdown(settings, period), 12, 22)},
        ),
    ]


def build_position_json(
    dashboard: DashboardDef, charts: dict[str, ChartDef], chart_ids: dict[str, int]
) -> dict[str, Any]:
    pos: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": dashboard.title}},
    }
    for r, row in enumerate(dashboard.layout):
        row_id = f"ROW-{r}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for key in row:
            parents = ["ROOT_ID", "GRID_ID", row_id]
            if key.startswith("md:"):
                text, width, height = dashboard.markdown[key[3:]]
                comp_id = f"MARKDOWN-{key[3:]}"
                pos[comp_id] = {
                    "type": "MARKDOWN",
                    "id": comp_id,
                    "children": [],
                    "parents": parents,
                    "meta": {"width": width, "height": height, "code": text},
                }
            else:
                chart = charts[key]
                comp_id = f"CHART-{key}"
                pos[comp_id] = {
                    "type": "CHART",
                    "id": comp_id,
                    "children": [],
                    "parents": parents,
                    "meta": {
                        "chartId": chart_ids[key],
                        "width": chart.width,
                        "height": chart.height,
                        "sliceName": chart.title,
                    },
                }
            pos[row_id]["children"].append(comp_id)
    return pos


# --- Provisionnement ---------------------------------------------------------------------------


def _ensure_database(client: SupersetClient, settings: Settings) -> int:
    uri = (
        f"trino://superset@{settings.trino_host}:{settings.trino_port}/"
        f"{settings.iceberg_catalog_name}"
    )
    for db in client.list_all("database"):
        if db.get("database_name") == DATABASE_NAME:
            return int(db["id"])
    created = client.request(
        "POST",
        "/api/v1/database/",
        json={
            "database_name": DATABASE_NAME,
            "sqlalchemy_uri": uri,
            "expose_in_sqllab": True,
            "allow_run_async": False,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False,
        },
    )
    log.info("Connexion Superset -> Trino créée (%s)", uri)
    return int(created["id"])


def _ensure_datasets(
    client: SupersetClient, settings: Settings, database_id: int
) -> dict[str, int]:
    existing = {
        (d.get("schema"), d.get("table_name")): int(d["id"]) for d in client.list_all("dataset")
    }
    ids: dict[str, int] = {}
    for ds in dataset_defs(settings):
        schema = settings.namespace(ds.schema)  # type: ignore[arg-type]
        found = existing.get((schema, ds.table))
        if found is not None:
            if ds.sql is not None:
                client.request("PUT", f"/api/v1/dataset/{found}", json={"sql": ds.sql})
            client.request("PUT", f"/api/v1/dataset/{found}/refresh")
            ids[ds.key] = found
            continue
        payload: dict[str, Any] = {
            "database": database_id,
            "schema": schema,
            "table_name": ds.table,
        }
        if ds.sql is not None:
            payload["sql"] = ds.sql
        ids[ds.key] = int(client.request("POST", "/api/v1/dataset/", json=payload)["id"])
        log.info("Dataset Superset créé : %s.%s", schema, ds.table)
    return ids


def _drop_dashboard(client: SupersetClient, slug: str) -> None:
    resp = client.session.get(
        f"{client.base_url}/api/v1/dashboard/{slug}", timeout=client.timeout_s
    )
    if resp.status_code == 404:
        return
    SupersetClient._check(resp, "GET dashboard")
    dash_id = resp.json()["result"]["id"]
    charts = client.request("GET", f"/api/v1/dashboard/{dash_id}/charts").get("result", [])
    client.request("DELETE", f"/api/v1/dashboard/{dash_id}")
    for chart in charts:
        client.request("DELETE", f"/api/v1/chart/{chart['id']}")
    log.info("Ancien dashboard %s supprimé (%d graphiques)", slug, len(charts))


def _publish(
    client: SupersetClient,
    dashboard: DashboardDef,
    charts: dict[str, ChartDef],
    dataset_ids: dict[str, int],
) -> int:
    _drop_dashboard(client, dashboard.slug)
    dash_id = int(
        client.request(
            "POST",
            "/api/v1/dashboard/",
            json={"dashboard_title": dashboard.title, "slug": dashboard.slug, "published": True},
        )["id"]
    )
    used = [key for row in dashboard.layout for key in row if not key.startswith("md:")]
    chart_ids: dict[str, int] = {}
    for key in used:
        chart = charts[key]
        ds_id = dataset_ids[chart.dataset]
        params = {**chart.params, "viz_type": chart.viz_type, "datasource": f"{ds_id}__table"}
        chart_ids[key] = int(
            client.request(
                "POST",
                "/api/v1/chart/",
                json={
                    "slice_name": chart.title,
                    "viz_type": chart.viz_type,
                    "datasource_id": ds_id,
                    "datasource_type": "table",
                    "params": json.dumps(params),
                    "description": chart.description,
                    "dashboards": [dash_id],
                },
            )["id"]
        )

    positions = build_position_json(dashboard, charts, chart_ids)
    metadata = {
        "positions": positions,
        "color_scheme": "supersetColors",
        "refresh_frequency": 0,
        "expanded_slices": {},
        "label_colors": {},
        "timed_refresh_immune_slices": [],
        "cross_filters_enabled": True,
        "default_filters": "{}",
    }
    client.request(
        "PUT",
        f"/api/v1/dashboard/{dash_id}",
        json={"position_json": json.dumps(positions), "json_metadata": json.dumps(metadata)},
    )
    return len(chart_ids)


def provision_dashboard(settings: Settings) -> dict[str, str]:
    """Crée/rafraîchit connexion, datasets, graphiques et dashboards.

    Retourne l'URL publique de chaque dashboard : ``{"activite": ..., "qualite": ...}``.
    """
    client = SupersetClient(
        settings.superset_url, settings.superset_admin_user, settings.superset_admin_password
    )
    client.wait_ready()
    client.login()

    database_id = _ensure_database(client, settings)
    dataset_ids = _ensure_datasets(client, settings, database_id)
    charts = {c.key: c for c in [*chart_defs(), *quality_chart_defs()]}

    base = settings.superset_public_url.rstrip("/")
    urls: dict[str, str] = {}
    period = loaded_period(settings)
    for dashboard, name in zip(
        dashboard_defs(settings, period), ("activite", "qualite"), strict=True
    ):
        n = _publish(client, dashboard, charts, dataset_ids)
        urls[name] = f"{base}/superset/dashboard/{dashboard.slug}/"
        log.info("Dashboard publié : %s (%d graphiques)", urls[name], n)
    return urls
