"""Génère ``tests/fixtures/sample_trips.csv`` : jeu d'exemple déterministe.

Sert (1) aux tests unitaires, (2) au mode hors ligne ``PIPELINE_SOURCE=sample``.
Période couverte : 2023-01-01 -> 2023-01-04 (fin exclue).

Contient 280 trajets valides réalistes + des cas « sales » dont les volumes sont
connus des tests (voir ``DIRTY_CASES``).

    python scripts/generate_sample_fixture.py
"""

from __future__ import annotations

import csv
import hashlib
import random
from datetime import datetime, timedelta
from pathlib import Path

COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
    "trip_miles", "pickup_census_tract", "dropoff_census_tract", "pickup_community_area",
    "dropoff_community_area", "fare", "tips", "tolls", "extras", "trip_total", "payment_type",
    "company", "pickup_centroid_latitude", "pickup_centroid_longitude", "pickup_centroid_location",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude", "dropoff_centroid_location",
]  # fmt: skip

# Centroïdes approximatifs de quelques community areas fréquentes.
AREAS = {
    8: (41.899602111, -87.633308037),  # Near North Side
    32: (41.878865584, -87.625192142),  # Loop
    28: (41.874005383, -87.66351755),  # Near West Side
    76: (41.979071301, -87.903039661),  # O'Hare
    6: (41.944226601, -87.655998182),  # Lake View
    7: (41.922686284, -87.649488729),  # Lincoln Park
    24: (41.901206994, -87.676355989),  # West Town
    33: (41.857495283, -87.620331856),  # Near South Side
    56: (41.785998518, -87.750934289),  # Garfield Ridge (Midway)
    3: (41.965812009, -87.655878786),  # Uptown
}
AREA_WEIGHTS = [30, 25, 15, 10, 6, 5, 4, 3, 1, 1]
COMPANIES = ["Flash Cab", "Taxi Affiliation Services", "Sun Taxi", "City Service", "Globe Taxi"]
PAYMENTS = ["Credit Card", "Cash", "Mobile", "Prcard"]
PAYMENT_WEIGHTS = [45, 30, 15, 10]
TS_FMT = "%Y-%m-%dT%H:%M:%S.000"

N_VALID = 280
DIRTY_CASES = {
    "duplicates": 3,  # copies exactes de trajets valides
    "missing_trip_id": 2,
    "end_before_start": 2,
    "negative_amount": 2,
    "invalid_start_ts": 1,
    "start_out_of_period": 1,
    # lignes conservées mais nettoyées / signalées
    "invalid_community_area": 2,
    "coords_out_of_bbox": 1,
    "duration_outlier": 2,
    "distance_outlier": 1,
    "amount_inconsistent": 1,
}


def _hash(*parts: object, length: int = 40) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:length]


def _point(lat: float, lon: float) -> str:
    return f"POINT ({lon:.10f} {lat:.9f})"


def _money(x: float) -> str:
    return f"{x:.2f}".rstrip("0").rstrip(".") if x else "0"


def valid_trip(rng: random.Random, i: int) -> dict[str, str]:
    day = datetime(2023, 1, 1) + timedelta(days=i % 3)
    start = day + timedelta(minutes=15 * rng.randrange(0, 96))
    minutes = rng.randint(5, 45)
    secs = minutes * 60 + rng.randint(0, 59)
    end = start + timedelta(minutes=15 * round(minutes / 15))
    miles = round(rng.uniform(0.3, minutes * 0.45), 2)
    fare = round(3.25 + 2.25 * miles + 0.25 * minutes, 2)
    payment = rng.choices(PAYMENTS, PAYMENT_WEIGHTS)[0]
    tips = round(fare * rng.choice([0.15, 0.18, 0.2]), 2) if payment != "Cash" else 0.0
    extras = rng.choice([0.0, 0.0, 1.0, 4.0])
    total = round(fare + tips + extras, 2)
    p_area, d_area = rng.choices(list(AREAS), AREA_WEIGHTS, k=2)
    (p_lat, p_lon), (d_lat, d_lon) = AREAS[p_area], AREAS[d_area]
    return {
        "trip_id": _hash("trip", i),
        "taxi_id": _hash("taxi", rng.randint(1, 60), length=128),
        "trip_start_timestamp": start.strftime(TS_FMT),
        "trip_end_timestamp": end.strftime(TS_FMT),
        "trip_seconds": str(secs),
        "trip_miles": str(miles),
        "pickup_census_tract": "",
        "dropoff_census_tract": "",
        "pickup_community_area": str(p_area),
        "dropoff_community_area": str(d_area),
        "fare": _money(fare),
        "tips": _money(tips),
        "tolls": "0",
        "extras": _money(extras),
        "trip_total": _money(total),
        "payment_type": payment,
        "company": rng.choice(COMPANIES),
        "pickup_centroid_latitude": f"{p_lat:.9f}",
        "pickup_centroid_longitude": f"{p_lon:.9f}",
        "pickup_centroid_location": _point(p_lat, p_lon),
        "dropoff_centroid_latitude": f"{d_lat:.9f}",
        "dropoff_centroid_longitude": f"{d_lon:.9f}",
        "dropoff_centroid_location": _point(d_lat, d_lon),
    }


def build_rows() -> list[dict[str, str]]:
    rng = random.Random(20230101)
    valid = [valid_trip(rng, i) for i in range(N_VALID)]
    rows = list(valid)
    n = N_VALID

    def dirty(tag: str, **overrides: str) -> dict[str, str]:
        nonlocal n
        base = valid_trip(rng, n)
        base["trip_id"] = _hash("dirty", tag, n)
        n += 1
        base.update(overrides)
        return base

    rows += [dict(valid[i]) for i in range(DIRTY_CASES["duplicates"])]
    rows += [dirty("noid", trip_id="") for _ in range(DIRTY_CASES["missing_trip_id"])]
    rows += [
        dirty(
            "inverted",
            trip_start_timestamp="2023-01-02T10:30:00.000",
            trip_end_timestamp="2023-01-02T09:45:00.000",
        )
        for _ in range(DIRTY_CASES["end_before_start"])
    ]
    for _ in range(DIRTY_CASES["negative_amount"]):
        row = dirty("negative")
        row["fare"] = "-" + row["fare"]
        rows.append(row)
    rows += [
        dirty("badts", trip_start_timestamp="not-a-date")
        for _ in range(DIRTY_CASES["invalid_start_ts"])
    ]
    rows += [
        dirty(
            "outofperiod",
            trip_start_timestamp="2022-12-31T23:45:00.000",
            trip_end_timestamp="2023-01-01T00:00:00.000",
        )
        for _ in range(DIRTY_CASES["start_out_of_period"])
    ]
    rows += [
        dirty("badarea", pickup_community_area="99"),
        dirty("badarea", dropoff_community_area="0"),
    ]
    rows += [
        dirty("badcoords", pickup_centroid_latitude="40.000000000")
        for _ in range(DIRTY_CASES["coords_out_of_bbox"])
    ]
    rows += [dirty("zerosecs", trip_seconds="0") for _ in range(DIRTY_CASES["duration_outlier"])]
    rows += [dirty("farfar", trip_miles="900") for _ in range(DIRTY_CASES["distance_outlier"])]
    rows += [
        dirty("badtotal", trip_total="999.99") for _ in range(DIRTY_CASES["amount_inconsistent"])
    ]
    # Valeurs à normaliser (restent valides) : espaces, chaîne vide, symbole monétaire.
    rows += [
        dirty("spaces", company="  Flash Cab  ", payment_type=""),
    ]
    money_row = dirty("dollar")
    money_row["fare"] = "$" + money_row["fare"]
    rows.append(money_row)
    return rows


def main() -> None:
    out = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "sample_trips.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    rows = build_rows()
    with out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} lignes écrites dans {out}")


if __name__ == "__main__":
    main()
