"""Configuration Superset du POC (lue via PYTHONPATH=/app/pythonpath)."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://"
    f"{os.environ.get('SUPERSET_DB_USER', 'superset')}:"
    f"{os.environ['SUPERSET_DB_PASSWORD']}"
    f"@{os.environ.get('POSTGRES_HOST', 'postgres')}:5432/superset"
)

# Interface en français par défaut (anglais disponible).
BABEL_DEFAULT_LOCALE = "fr"
LANGUAGES = {
    "fr": {"flag": "fr", "name": "Français"},
    "en": {"flag": "us", "name": "English"},
}

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": False,
}

# POC local en HTTP : pas d'en-têtes de sécurité HTTPS forcés.
TALISMAN_ENABLED = False

# Cache des résultats de requêtes (les tables gold ne changent qu'à chaque run).
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_data_",
}

SUPERSET_WEBSERVER_TIMEOUT = 120
ROW_LIMIT = 50000
