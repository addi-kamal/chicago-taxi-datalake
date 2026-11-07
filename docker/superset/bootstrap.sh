#!/usr/bin/env bash
# Démarrage idempotent de Superset : migrations, compte admin, rôles, serveur.
# Le dashboard lui-même est provisionné par le pipeline (tâche `publish_dashboard`).
set -euo pipefail

superset db upgrade
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-superset_admin}" \
  --firstname Superset \
  --lastname Datalake \
  --email "superset@datalake.local" \
  --password "${SUPERSET_ADMIN_PASSWORD:?SUPERSET_ADMIN_PASSWORD manquant}" || echo "Compte admin déjà présent"
superset init

exec /usr/bin/run-server.sh
