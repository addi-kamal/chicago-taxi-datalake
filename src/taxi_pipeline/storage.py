"""Accès S3 générique (boto3) - compatible MinIO et Ceph RGW.

Règles de portabilité :
- endpoint + credentials + région uniquement, lus depuis la configuration ;
- adressage *path-style* (aucune résolution DNS de bucket) ;
- checksums envoyés uniquement lorsqu'ils sont requis (compatibilité Ceph RGW) ;
- aucune API spécifique à MinIO.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from taxi_pipeline.config import Settings

log = logging.getLogger(__name__)


class ObjectStore(Protocol):
    """Interface minimale utilisée par le code métier (facilite les tests)."""

    def exists(self, bucket: str, key: str) -> bool: ...

    def upload_file(self, local_path: str, bucket: str, key: str) -> None: ...

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None: ...

    def get_json(self, bucket: str, key: str) -> dict[str, Any]: ...


def build_s3_client(settings: Settings) -> BaseClient:
    config = Config(
        s3={"addressing_style": "path" if settings.s3_path_style else "auto"},
        signature_version="s3v4",
        retries={"max_attempts": 5, "mode": "standard"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        use_ssl=settings.s3_use_ssl,
        config=config,
    )


class S3ObjectStore:
    """Implémentation boto3 de :class:`ObjectStore`."""

    def __init__(self, settings: Settings, client: BaseClient | None = None) -> None:
        self._settings = settings
        self._client = client or build_s3_client(settings)

    @property
    def client(self) -> BaseClient:
        return self._client

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    def upload_file(self, local_path: str, bucket: str, key: str) -> None:
        self._client.upload_file(local_path, bucket, key)

    def put_json(self, bucket: str, key: str, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True, default=str).encode("utf-8")
        self._client.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")

    def get_json(self, bucket: str, key: str) -> dict[str, Any]:
        obj = self._client.get_object(Bucket=bucket, Key=key)
        data: dict[str, Any] = json.loads(obj["Body"].read())
        return data

    def ensure_bucket(self, bucket: str) -> bool:
        """Crée le bucket s'il n'existe pas. Retourne True si créé."""
        try:
            self._client.head_bucket(Bucket=bucket)
            return False
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code not in {"404", "NoSuchBucket", "NotFound"}:
                raise
        params: dict[str, Any] = {"Bucket": bucket}
        if self._settings.s3_region != "us-east-1":
            params["CreateBucketConfiguration"] = {"LocationConstraint": self._settings.s3_region}
        self._client.create_bucket(**params)
        log.info("Bucket créé : %s", bucket)
        return True
