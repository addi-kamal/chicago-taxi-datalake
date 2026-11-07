"""Smoke test de la couche d'exposition : interroge Trino et affiche un aperçu des KPIs.

Échoue (code 1) si une table gold est vide.

    docker compose exec airflow-scheduler python /opt/airflow/project/scripts/smoke_test.py
    # ou : make smoke
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from taxi_pipeline.config import get_settings
from taxi_pipeline.serving.trino_checks import query, wait_for_trino
from taxi_pipeline.tables import ALL_TABLES

PREVIEWS = {
    "CA et volumes par mois": """
        SELECT trip_month, sum(trips) AS trips, sum(total_revenue) AS revenue,
               round(sum(tips_revenue) / sum(fare_revenue), 4) AS tip_rate
        FROM {gold}.daily_revenue GROUP BY 1 ORDER BY 1""",
    "Top 5 zones (dernier mois)": """
        SELECT community_area_name, trips, total_revenue
        FROM {gold}.top_pickup_areas
        WHERE trip_month = (SELECT max(trip_month) FROM {gold}.top_pickup_areas)
        ORDER BY area_rank LIMIT 5""",
    "Moyens de paiement": """
        SELECT payment_type, sum(trips) AS trips,
               round(100 * CAST(sum(trips) AS double) / sum(sum(trips)) OVER (), 1) AS pct
        FROM {gold}.payment_mix GROUP BY 1 ORDER BY 2 DESC""",
}


def main() -> int:
    settings = get_settings()
    wait_for_trino(settings, timeout_s=60)
    gold = f"{settings.iceberg_catalog_name}.{settings.namespace('gold')}"

    print(f"{'table':<45} {'lignes':>12}")
    empty_gold = []
    for spec in ALL_TABLES:
        table = settings.table(spec.layer, spec.name)
        n = int(query(settings, f"SELECT count(*) FROM {table}")[0][0])
        print(f"{table:<45} {n:>12,}")
        if spec.layer == "gold" and n == 0:
            empty_gold.append(table)

    for title, sql in PREVIEWS.items():
        print(f"\n== {title}")
        for row in query(settings, sql.format(gold=gold)):
            print("   " + " | ".join(str(v) for v in row))

    if empty_gold:
        print(f"\nÉCHEC : tables gold vides : {empty_gold}")
        return 1
    print("\nSmoke test OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
