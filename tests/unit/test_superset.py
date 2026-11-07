"""Définition des dashboards Superset (as code) : cohérence de la mise en page."""

from __future__ import annotations

import pytest

from taxi_pipeline.bi.superset import (
    DASHBOARD_SLUG,
    QUALITY_SLUG,
    DashboardDef,
    build_position_json,
    chart_defs,
    dashboard_defs,
    dataset_defs,
    quality_chart_defs,
    rison,
)
from taxi_pipeline.config import Settings

ALL_CHARTS = {c.key: c for c in [*chart_defs(), *quality_chart_defs()]}
DASHBOARDS = dashboard_defs(Settings())


def test_rison_encoding() -> None:
    assert rison({"page": 0, "page_size": 100}) == "(page:0,page_size:100)"
    assert (
        rison({"filters": [{"col": "x", "value": "it's!"}]})
        == "(filters:!((col:'x',value:'it!'s!!')))"
    )
    assert rison(True) == "!t"


def test_chart_keys_are_unique() -> None:
    assert len(ALL_CHARTS) == len(chart_defs()) + len(quality_chart_defs())


def test_two_dashboards_activity_and_quality() -> None:
    assert [d.slug for d in DASHBOARDS] == [DASHBOARD_SLUG, QUALITY_SLUG]


def _placed(dashboard: DashboardDef) -> list[str]:
    return [k for row in dashboard.layout for k in row if not k.startswith("md:")]


def test_each_chart_belongs_to_exactly_one_dashboard() -> None:
    activity, quality = (_placed(d) for d in DASHBOARDS)
    assert sorted(activity) == sorted(c.key for c in chart_defs())
    assert sorted(quality) == sorted(c.key for c in quality_chart_defs())


def test_activity_dashboard_has_no_quality_chart() -> None:
    activity = DASHBOARDS[0]
    assert not {ALL_CHARTS[k].dataset for k in _placed(activity)} & {
        "dq_latest",
        "layer_volumes",
        "rejected",
        "dq_flags_daily",
        "dq_history",
    }


@pytest.mark.parametrize("dashboard", DASHBOARDS, ids=lambda d: d.slug)
def test_rows_fit_the_grid_and_markdown_exists(dashboard: DashboardDef) -> None:
    for row in dashboard.layout:
        width = sum(12 if k.startswith("md:") else ALL_CHARTS[k].width for k in row)
        assert width <= 12, row
        for key in row:
            if key.startswith("md:"):
                assert key[3:] in dashboard.markdown


def test_charts_reference_declared_datasets() -> None:
    datasets = {d.key for d in dataset_defs(Settings())}
    assert {c.dataset for c in ALL_CHARTS.values()} <= datasets


@pytest.mark.parametrize("dashboard", DASHBOARDS, ids=lambda d: d.slug)
def test_position_json_structure(dashboard: DashboardDef) -> None:
    ids = {k: i for i, k in enumerate(_placed(dashboard), start=100)}
    pos = build_position_json(dashboard, ALL_CHARTS, ids)
    assert pos["HEADER_ID"]["meta"]["text"] == dashboard.title
    chart_nodes = [v for v in pos.values() if isinstance(v, dict) and v.get("type") == "CHART"]
    assert sorted(n["meta"]["chartId"] for n in chart_nodes) == sorted(ids.values())
    for node_id, node in pos.items():
        if isinstance(node, dict):
            for child in node.get("children", []):
                assert child in pos, f"{node_id} -> {child}"
