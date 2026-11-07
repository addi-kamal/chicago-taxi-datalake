"""Accès aux données : une seule fonction, ``fetch(sql, params)``, au-dessus de Trino.

Pourquoi Trino et pas Spark ou les fichiers Parquet directement : Trino est déjà la
couche de lecture du datalake (Superset, DBeaver). L'API lit donc **les mêmes tables,
avec le même moteur** que le dashboard : un chiffre affiché ici est identique au
chiffre du dashboard.

Les valeurs fournies par l'utilisateur sont toujours passées en **paramètres**
(``?``, requêtes préparées Trino), jamais concaténées dans le SQL : pas d'injection.
"""

from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Protocol

import requests
import trino
from trino.exceptions import TrinoConnectionError, TrinoQueryError

# Erreurs Trino qui signifient « les données ne sont pas (encore) là » : par exemple
# l'API démarrée avant la fin de l'initialisation du datalake.
_NOT_READY_ERRORS = {"TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND", "CATALOG_NOT_FOUND"}


class DataUnavailableError(Exception):
    """Trino injoignable ou tables absentes : l'API répond 503."""


class Database(Protocol):
    def fetch(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class TrinoDatabase:
    host: str
    port: int
    user: str
    catalog: str
    schema: str = "gold"
    timeout_s: float = 30.0

    def fetch(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        try:
            # Une connexion par requête : le client Trino parle HTTP, sans état à garder.
            conn = trino.dbapi.connect(
                host=self.host,
                port=self.port,
                # Utilisateur visible dans l'UI Trino : on y repère les requêtes de l'API.
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                http_scheme="http",
                request_timeout=self.timeout_s,
            )
            try:
                cur = conn.cursor()
                cur.execute(sql, list(params) if params else None)
                rows = cur.fetchall()
                names = [col[0] for col in cur.description]
            finally:
                conn.close()
        except (TrinoConnectionError, requests.exceptions.RequestException) as exc:
            raise DataUnavailableError("Trino est injoignable") from exc
        except TrinoQueryError as exc:
            if exc.error_name in _NOT_READY_ERRORS:
                raise DataUnavailableError(
                    "Tables gold absentes : le pipeline n'a pas encore tourné"
                ) from exc
            raise
        return [dict(zip(names, row, strict=True)) for row in rows]


@lru_cache(maxsize=1)
def get_db() -> Database:
    """Dépendance FastAPI (remplacée par une fausse base dans les tests)."""
    return TrinoDatabase(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "api"),
        catalog=os.environ.get("TRINO_CATALOG", "iceberg"),
    )
