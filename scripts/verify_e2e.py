"""Vérification de bout en bout d'une installation (à lancer depuis la racine du repo).

Contrôle, dans l'ordre :
  1. prérequis    : fichier .env complet, Docker joignable ;
  2. services     : les 8 services Docker Compose sont « healthy », init terminé avec succès ;
  3. interfaces   : Airflow, Trino, Superset, MinIO, DBeaver, API répondent en HTTP ;
  4. pipeline     : le DAG chicago_taxi_e2e se termine en succès (attente si en cours) ;
  5. données      : volumes cohérents entre bronze, silver et gold, lus via Trino ;
  6. dashboards   : les dashboards Superset (activité, qualité) existent avec leurs graphiques ;
  7. DBeaver      : connexion Trino préconfigurée, requête SQL exécutée sur gold ;
  8. API          : endpoints gold en 200, KPIs de l'API = volumes silver lus par Trino ;
  9. (option)     : tests unitaires, d'intégration et de l'API exécutés dans les conteneurs.

Uniquement la bibliothèque standard Python : aucun paquet à installer sur le poste.

    python scripts/verify_e2e.py                 # attend la fin du pipeline (60 min max)
    python scripts/verify_e2e.py --with-tests    # + tests unitaires et d'intégration (~3 min)
    python scripts/verify_e2e.py --no-wait       # n'attend pas : état instantané

Code de sortie 0 si tout est OK, 1 sinon.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DAG_ID = "chicago_taxi_e2e"
EXPECTED_SERVICES = (
    "postgres",
    "minio",
    "trino",
    "airflow-webserver",
    "airflow-scheduler",
    "superset",
    "dbeaver",
    "api",
)
REQUIRED_SECRETS = (
    "S3_SECRET_KEY",
    "ICEBERG_CATALOG_PASSWORD",
    "POSTGRES_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "SUPERSET_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "SUPERSET_ADMIN_PASSWORD",
    "AIRFLOW_SECRET_KEY",
    "SUPERSET_SECRET_KEY",
    "AIRFLOW_FERNET_KEY",
    "DBEAVER_ADMIN_PASSWORD",
)
EXPECTED_CHARTS = {"chicago-taxi": 11, "chicago-taxi-quality": 10}

# Les chemins /opt/... passés à `docker compose exec` ne doivent pas être convertis par Git Bash.
os.environ.setdefault("MSYS_NO_PATHCONV", "1")
if sys.stdout.encoding and sys.stdout.encoding.lower() not in {"utf-8", "utf8"}:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]


class Report:
    def __init__(self) -> None:
        self.failures = 0

    def section(self, title: str) -> None:
        print(f"\n== {title}")

    def check(self, ok: bool, label: str, detail: str = "") -> bool:
        mark = "OK " if ok else "KO "
        print(f"  [{mark}] {label}" + (f" - {detail}" if detail else ""))
        if not ok:
            self.failures += 1
        return ok


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                env[key.strip()] = value.strip()
    return env


def compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def psql(sql: str, database: str = "airflow") -> str:
    res = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        f'psql -U "$POSTGRES_USER" -d {database} -At -c "{sql}"',
    )
    return res.stdout.strip() if res.returncode == 0 else ""


def http_ok(url: str, timeout: int = 10) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300, f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, type(exc).__name__


# --- Étapes -------------------------------------------------------------------------


def check_prerequisites(r: Report, env: dict[str, str]) -> bool:
    r.section("1. Prérequis")
    r.check(shutil.which("docker") is not None, "commande docker disponible")
    missing = [k for k in REQUIRED_SECRETS if not env.get(k)]
    r.check(
        not missing,
        "fichier .env complet",
        f"manquant : {missing} -> lancer `python scripts/generate_env.py`" if missing else "",
    )
    res = compose("version", timeout=30)
    return r.check(res.returncode == 0, "Docker Compose v2 joignable", res.stdout.strip()[:60])


def service_states() -> dict[str, dict[str, object]]:
    res = compose("ps", "-a", "--format", "json")
    rows: list[dict[str, object]] = []
    for line in res.stdout.splitlines():
        line = line.strip()
        if line.startswith("["):
            rows.extend(json.loads(line))
        elif line.startswith("{"):
            rows.append(json.loads(line))
    return {str(row.get("Service")): row for row in rows}


def check_services(r: Report, wait_minutes: int) -> None:
    r.section("2. Services Docker")
    # Juste après `docker compose up`, les services sont encore « starting » : on attend
    # qu'ils deviennent healthy (ou qu'un healthcheck échoue) avant de conclure.
    deadline = time.monotonic() + wait_minutes * 60
    by_service = service_states()
    while time.monotonic() < deadline:
        pending = [
            n
            for n in EXPECTED_SERVICES
            if str((by_service.get(n) or {}).get("Health") or "") in {"starting", ""}
        ]
        if not pending:
            break
        print(f"     … en démarrage : {', '.join(pending)}")
        time.sleep(15)
        by_service = service_states()
    for name in EXPECTED_SERVICES:
        row = by_service.get(name)
        health = str(row.get("Health") or row.get("State")) if row else "absent"
        r.check(health == "healthy", f"{name}", health)
    init = by_service.get("init")
    exit_code = init.get("ExitCode") if init else None
    r.check(
        init is not None and str(init.get("State")) == "exited" and exit_code == 0,
        "init terminé avec succès",
        f"état={init.get('State')} code={exit_code}" if init else "absent",
    )


def check_endpoints(r: Report, env: dict[str, str]) -> None:
    r.section("3. Interfaces web (depuis le poste)")
    endpoints = {
        "Airflow   http://localhost:{p}/health": ("AIRFLOW_HOST_PORT", "8081", "/health"),
        "Trino     http://localhost:{p}/v1/info": ("TRINO_HOST_PORT", "8080", "/v1/info"),
        "Superset  http://localhost:{p}/health": ("SUPERSET_HOST_PORT", "8088", "/health"),
        "MinIO     http://localhost:{p}/minio/health/live": (
            "MINIO_API_HOST_PORT",
            "9000",
            "/minio/health/live",
        ),
        "DBeaver   http://localhost:{p}/": ("DBEAVER_HOST_PORT", "8978", "/"),
        "API       http://localhost:{p}/health/ready": ("API_HOST_PORT", "8000", "/health/ready"),
    }
    for label, (var, default, path) in endpoints.items():
        port = env.get(var) or default
        ok, detail = http_ok(f"http://localhost:{port}{path}")
        r.check(ok, label.format(p=port), detail)


def task_summary() -> tuple[str, dict[str, str]]:
    run = psql(
        f"select state || '|' || run_id from dag_run where dag_id='{DAG_ID}' "
        "order by id desc limit 1"
    )
    if not run:
        return "no_run", {}
    state, run_id = run.split("|", 1)
    rows = psql(
        "select task_id, string_agg(coalesce(state,'none') || '=' || n, ' ') from "
        "(select task_id, state, count(*) n from task_instance "
        f"where dag_id='{DAG_ID}' and run_id='{run_id}' group by 1,2) t group by task_id"
    )
    tasks = dict(line.split("|", 1) for line in rows.splitlines() if "|" in line)
    return state, tasks


def check_pipeline(r: Report, wait_minutes: int) -> bool:
    r.section("4. Pipeline Airflow")
    import_errors = psql("select count(*) from import_error")
    r.check(import_errors == "0", "DAGs chargés sans erreur", f"{import_errors or '?'} erreur(s)")

    deadline = time.monotonic() + wait_minutes * 60
    last = ""
    state, tasks = task_summary()
    while state in {"no_run", "queued", "running"} and time.monotonic() < deadline:
        progress = tasks.get("extract_day", "")
        current = f"état={state} extraction[{progress}]"
        if current != last:
            print(f"     … {current}")
            last = current
        time.sleep(30)
        state, tasks = task_summary()

    ok = r.check(state == "success", f"dernier run de {DAG_ID}", state)
    for task_id in sorted(tasks):
        if any(s in tasks[task_id] for s in ("failed", "upstream_failed")):
            r.check(False, f"tâche {task_id}", tasks[task_id])
    if not ok and state in {"no_run", "queued", "running"}:
        print("     (pipeline pas encore terminé : relancer le script plus tard)")
    return ok


def trino_count(table: str) -> int:
    res = compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--catalog",
        "iceberg",
        "--output-format",
        "TSV",
        "--execute",
        f"SELECT count(*) FROM {table}",
    )
    try:
        return int(res.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return -1


def check_data(r: Report) -> None:
    r.section("5. Données (lues par Trino)")
    counts = {
        t: trino_count(t)
        for t in (
            "bronze.taxi_trips_raw",
            "silver.taxi_trips",
            "silver.taxi_trips_rejected",
            "silver.dim_community_area",
            "gold.daily_revenue",
            "gold.trip_duration_stats",
            "gold.top_pickup_areas",
            "gold.payment_mix",
            "gold.company_performance",
        )
    }
    for table, n in counts.items():
        r.check(n > 0 or (table.endswith("_rejected") and n >= 0), f"{table:<28}", f"{n:,} lignes")
    bronze, silver, rejected = (
        counts["bronze.taxi_trips_raw"],
        counts["silver.taxi_trips"],
        counts["silver.taxi_trips_rejected"],
    )
    r.check(
        bronze > 0 and silver + rejected <= bronze,
        "cohérence bronze ≥ silver + quarantaine",
        f"{bronze:,} ≥ {silver:,} + {rejected:,}",
    )
    r.check(counts["silver.dim_community_area"] == 77, "référentiel : 77 community areas")


def check_dashboard(r: Report, env: dict[str, str]) -> None:
    r.section("6. Dashboards Superset")
    base = f"http://localhost:{env.get('SUPERSET_HOST_PORT') or '8088'}"
    user = env.get("SUPERSET_ADMIN_USER") or "superset_admin"
    try:
        req = urllib.request.Request(
            f"{base}/api/v1/security/login",
            data=json.dumps(
                {
                    "username": user,
                    "password": env.get("SUPERSET_ADMIN_PASSWORD", ""),
                    "provider": "db",
                }
            ).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            token = json.load(resp)["access_token"]
        r.check(True, f"connexion API avec le compte {user}")
        auth = {"Authorization": f"Bearer {token}"}
        get: Callable[[str], dict[str, object]] = lambda path: json.load(  # noqa: E731
            urllib.request.urlopen(
                urllib.request.Request(f"{base}{path}", headers=auth), timeout=30
            )
        )
        for slug, expected in EXPECTED_CHARTS.items():
            charts = get(f"/api/v1/dashboard/{slug}/charts").get("result", [])
            n = len(charts) if isinstance(charts, list) else 0
            r.check(n >= expected, f"dashboard {slug}", f"{n} graphiques")
            print(f"     -> {base}/superset/dashboard/{slug}/")
    except Exception as exc:
        r.check(False, "accès au dashboard", f"{type(exc).__name__}: {exc}")


def check_dbeaver(r: Report, env: dict[str, str]) -> None:
    """Connexion à CloudBeaver, connexion Trino préconfigurée, requête SQL réelle."""
    r.section("7. DBeaver (CloudBeaver) -> Trino")
    url = f"http://localhost:{env.get('DBEAVER_HOST_PORT') or '8978'}/api/gql"
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())

    def gql(query: str, variables: dict[str, object] | None = None) -> dict[str, object]:
        body = json.dumps({"query": query, "variables": variables or {}}).encode()
        req = urllib.request.Request(url, body, {"Content-Type": "application/json"})
        data = json.load(opener.open(req, timeout=60))
        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"])[:300])
        return dict(data["data"])

    try:
        user = env.get("DBEAVER_ADMIN_USER") or "cbadmin"
        # Le fournisseur « local » attend l'empreinte MD5 du mot de passe (comme le client web).
        pwd = hashlib.md5(env.get("DBEAVER_ADMIN_PASSWORD", "").encode()).hexdigest().upper()
        login = gql(
            'query($c: Object){ authLogin(provider:"local", credentials:$c){ authStatus } }',
            {"c": {"user": user, "password": pwd}},
        )
        status = login["authLogin"]["authStatus"]  # type: ignore[index]
        r.check(status == "SUCCESS", f"connexion avec le compte {user}", str(status))
        conns = gql("query { userConnections { id name driverId projectId readOnly } }")
        trino = [c for c in conns["userConnections"] if "trino" in c["driverId"]]  # type: ignore[union-attr]
        if not r.check(
            bool(trino), "connexion Trino préconfigurée", str([c["name"] for c in trino])
        ):
            return
        conn = trino[0]
        ids = {"id": conn["id"], "p": conn["projectId"]}
        gql("mutation($id: ID!, $p: ID){ initConnection(id:$id, projectId:$p){ connected } }", ids)
        ctx = gql(
            "mutation($id: ID!, $p: ID){ sqlContextCreate(connectionId:$id, projectId:$p){ id } }",
            ids,
        )["sqlContextCreate"]["id"]  # type: ignore[index]
        task = gql(
            "mutation($p: ID, $c: ID!, $x: ID!, $s: String!){ asyncSqlExecuteQuery("
            "projectId:$p, connectionId:$c, contextId:$x, sql:$s){ id } }",
            {
                "p": conn["projectId"],
                "c": conn["id"],
                "x": ctx,
                "s": "SELECT count(*) FROM gold.daily_revenue",
            },
        )["asyncSqlExecuteQuery"]["id"]  # type: ignore[index]
        for _ in range(60):
            info = gql(
                "mutation($id: String!){ asyncTaskInfo(id:$id, removeOnFinish:false)"
                "{ running error{ message } } }",
                {"id": task},
            )["asyncTaskInfo"]
            if not info["running"]:  # type: ignore[index]
                break
            time.sleep(1)
        if info.get("error"):  # type: ignore[union-attr]
            raise RuntimeError(str(info["error"]))  # type: ignore[index]
        res = gql(
            "mutation($id: ID!){ asyncSqlExecuteResults(taskId:$id)"
            "{ results{ resultSet{ rowsWithMetaData{ data } } } } }",
            {"id": task},
        )
        rows = res["asyncSqlExecuteResults"]["results"][0]["resultSet"]["rowsWithMetaData"]  # type: ignore[index]
        value = int(rows[0]["data"][0])
        r.check(value > 0, "requête SQL via DBeaver sur gold.daily_revenue", f"{value} lignes")
        print(f"     -> http://localhost:{env.get('DBEAVER_HOST_PORT') or '8978'}/")
    except Exception as exc:
        r.check(False, "accès à DBeaver", f"{type(exc).__name__}: {exc}")


def check_api(r: Report, env: dict[str, str]) -> None:
    """Endpoints de l'API gold, et cohérence de ses chiffres avec silver (via Trino)."""
    r.section("8. API gold (FastAPI)")
    base = f"http://localhost:{env.get('API_HOST_PORT') or '8000'}"
    for path in (
        "/v1/metadata",
        "/v1/revenue/daily",
        "/v1/revenue/monthly",
        "/v1/durations/by-hour",
        "/v1/areas/top",
        "/v1/payments/mix",
        "/v1/companies",
        "/v1/quality/latest",
    ):
        ok, detail = http_ok(f"{base}{path}", timeout=60)
        r.check(ok, f"GET {path}", detail)
    try:
        with urllib.request.urlopen(f"{base}/v1/kpis", timeout=60) as resp:
            api_trips = int(json.load(resp)["trips"])
        silver = trino_count("silver.taxi_trips")
        r.check(
            api_trips == silver,
            "trajets /v1/kpis = lignes silver.taxi_trips",
            f"{api_trips:,} = {silver:,}",
        )
        print(f"     -> {base}/docs")
    except Exception as exc:
        r.check(False, "GET /v1/kpis", f"{type(exc).__name__}: {exc}")


def check_tests(r: Report) -> None:
    r.section("9. Tests automatisés (dans les conteneurs)")
    for label, cmd in (
        ("tests unitaires", "python -m pytest tests/unit -q -p no:cacheprovider"),
        (
            "tests d'intégration (round-trip Spark/Trino, idempotence)",
            "RUN_INTEGRATION=1 python -m pytest tests/integration -q -p no:cacheprovider",
        ),
    ):
        res = compose(
            "exec",
            "-T",
            "airflow-scheduler",
            "bash",
            "-c",
            f"cd /opt/airflow/project && {cmd}",
            timeout=1200,
        )
        summary = next(
            (ln for ln in reversed(res.stdout.splitlines()) if "passed" in ln or "failed" in ln),
            res.stdout.strip()[-200:],
        )
        r.check(res.returncode == 0, label, summary.strip())
    res = compose("exec", "-T", "api", "python", "-m", "pytest", "-q", "-p", "no:cacheprovider")
    summary = next(
        (ln for ln in reversed(res.stdout.splitlines()) if "passed" in ln or "failed" in ln),
        res.stdout.strip()[-200:],
    )
    r.check(res.returncode == 0, "tests de l'API", summary.strip())


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--with-tests", action="store_true", help="lance aussi les tests")
    parser.add_argument("--no-wait", action="store_true", help="n'attend pas la fin du pipeline")
    parser.add_argument("--wait-minutes", type=int, default=60)
    args = parser.parse_args()

    r = Report()
    env = load_env()
    if not check_prerequisites(r, env):
        print("\nDocker n'est pas joignable : démarrer Docker Desktop puis relancer.")
        return 1
    check_services(r, 0 if args.no_wait else 10)
    check_endpoints(r, env)
    pipeline_ok = check_pipeline(r, 0 if args.no_wait else args.wait_minutes)
    if pipeline_ok:
        check_data(r)
        check_dashboard(r, env)
        check_dbeaver(r, env)
        check_api(r, env)
        if args.with_tests:
            check_tests(r)

    print("\n" + "=" * 60)
    if r.failures:
        print(f"RÉSULTAT : {r.failures} contrôle(s) en échec - voir README, section Dépannage")
        return 1
    print("RÉSULTAT : tout est opérationnel ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
