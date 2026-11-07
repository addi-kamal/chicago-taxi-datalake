"""Génère le fichier ``.env`` local à partir de ``.env.example``.

Chaque secret (mots de passe, clés de session, clé Fernet Airflow) reçoit une
valeur aléatoire tirée de ``secrets`` ; les autres paramètres reprennent les
valeurs de ``.env.example``. Aucun secret n'est versionné : ``.env`` est ignoré
par git.

Usage :
    python scripts/generate_env.py            # crée .env s'il n'existe pas
    python scripts/generate_env.py --force    # régénère (implique docker compose down -v)
"""

from __future__ import annotations

import argparse
import base64
import os
import secrets
import string
import sys
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / ".env.example"
TARGET = ROOT / ".env"

# Alphanumérique uniquement : les mots de passe sont injectés dans des URL
# SQLAlchemy/JDBC et dans des commandes shell, sans échappement.
_ALPHABET = string.ascii_letters + string.digits

PASSWORD_LENGTH = 32
SESSION_KEY_LENGTH = 64


def random_password(length: int = PASSWORD_LENGTH) -> str:
    return "".join(secrets.choice(_ALPHABET) for _ in range(length))


def policy_password(length: int = PASSWORD_LENGTH) -> str:
    """Mot de passe garanti avec majuscule, minuscule et chiffre (politique CloudBeaver)."""
    while True:
        value = random_password(length)
        if (
            any(c.islower() for c in value)
            and any(c.isupper() for c in value)
            and any(c.isdigit() for c in value)
        ):
            return value


def fernet_key() -> str:
    """Clé Fernet (32 octets, base64 url-safe), format attendu par Airflow."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


GENERATORS: dict[str, Callable[[], str]] = {
    "S3_SECRET_KEY": random_password,
    "ICEBERG_CATALOG_PASSWORD": random_password,
    "POSTGRES_PASSWORD": random_password,
    "AIRFLOW_DB_PASSWORD": random_password,
    "SUPERSET_DB_PASSWORD": random_password,
    "AIRFLOW_ADMIN_PASSWORD": random_password,
    "SUPERSET_ADMIN_PASSWORD": random_password,
    "AIRFLOW_SECRET_KEY": lambda: random_password(SESSION_KEY_LENGTH),
    "SUPERSET_SECRET_KEY": lambda: random_password(SESSION_KEY_LENGTH),
    "AIRFLOW_FERNET_KEY": fernet_key,
    "DBEAVER_ADMIN_PASSWORD": policy_password,
}


def render(template: str) -> tuple[str, dict[str, str]]:
    generated: dict[str, str] = {}
    lines = []
    for line in template.splitlines():
        key, sep, _ = line.partition("=")
        key = key.strip()
        if sep and not key.startswith("#") and key in GENERATORS:
            generated[key] = GENERATORS[key]()
            line = f"{key}={generated[key]}"
        lines.append(line)
    missing = set(GENERATORS) - set(generated)
    if missing:
        raise SystemExit(f".env.example incomplet, clés absentes : {', '.join(sorted(missing))}")
    return "\n".join(lines) + "\n", generated


def complete_existing() -> int:
    """Ajoute à un .env existant les clés apparues depuis dans .env.example.

    Les valeurs déjà présentes ne sont jamais modifiées : les volumes existants
    (bases, MinIO) restent compatibles, aucun `down -v` n'est nécessaire.
    """
    current = TARGET.read_text(encoding="utf-8")
    present = {
        line.partition("=")[0].strip()
        for line in current.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    added: list[str] = []
    for line in TEMPLATE.read_text(encoding="utf-8").splitlines():
        key, sep, default = line.partition("=")
        key = key.strip()
        if not sep or key.startswith("#") or key in present:
            continue
        value = GENERATORS[key]() if key in GENERATORS else default
        added.append(f"{key}={value}")
    if not added:
        print(f"{TARGET.name} existe déjà et est complet (--force pour tout régénérer).")
        return 0
    suffix = "" if current.endswith("\n") else "\n"
    block = "\n# --- Ajouté par generate_env.py (nouvelles clés) ---\n" + "\n".join(added) + "\n"
    TARGET.write_text(current + suffix + block, encoding="utf-8", newline="\n")
    print(f"{TARGET.name} complété : {', '.join(a.partition('=')[0] for a in added)}")
    for entry in added:
        if entry.startswith("DBEAVER_ADMIN_PASSWORD="):
            print(f"  DBeaver  : {entry.partition('=')[2]}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--force", action="store_true", help="écrase un .env existant")
    args = parser.parse_args()

    if TARGET.exists() and not args.force:
        return complete_existing()

    content, generated = render(TEMPLATE.read_text(encoding="utf-8"))
    TARGET.write_text(content, encoding="utf-8", newline="\n")
    if os.name == "posix":
        TARGET.chmod(0o600)

    values = {
        key: value
        for key, sep, value in (line.partition("=") for line in content.splitlines())
        if sep and not key.startswith("#")
    }
    print(f"{TARGET.name} généré ({len(generated)} secrets).")
    print(f"  Airflow  : {values['AIRFLOW_ADMIN_USER']} / {values['AIRFLOW_ADMIN_PASSWORD']}")
    print(f"  Superset : {values['SUPERSET_ADMIN_USER']} / {values['SUPERSET_ADMIN_PASSWORD']}")
    print(f"  MinIO    : {values['S3_ACCESS_KEY']} / {values['S3_SECRET_KEY']}")
    print(f"  DBeaver  : {values['DBEAVER_ADMIN_USER']} / {values['DBEAVER_ADMIN_PASSWORD']}")
    if args.force:
        print("Secrets modifiés : réinitialiser les volumes (docker compose down -v).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
