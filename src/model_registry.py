from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

import boto3


@dataclass(frozen=True)
class ModelRegistryEntry:
    model_name: str
    version: int
    run_id: str
    metric_auc: float
    model_uri: str
    created_at: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an s3 uri: {uri}")
    bucket_and_key = uri[len("s3://") :]
    bucket, _, key = bucket_and_key.partition("/")
    if not bucket or not key:
        raise ValueError(f"Invalid s3 uri: {uri}")
    return bucket, key


def _read_json_from_s3(uri: str) -> Any:
    bucket, key = _parse_s3_uri(uri)
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None


def _write_json_to_s3(uri: str, data: Any) -> None:
    bucket, key = _parse_s3_uri(uri)
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode("utf-8"))


def register_model(
    *,
    registry_uri: str,
    model_name: str,
    run_id: str,
    metric_auc: float,
    model_uri: str,
) -> ModelRegistryEntry:
    """
    Lightweight "model registry" that appends entries to a JSON file.

    - registry_uri can be a local path (e.g., ./model_registry.json) OR an S3 URI
      (e.g., s3://my-bucket/models/model_registry.json).
    """
    is_s3 = registry_uri.startswith("s3://")

    if is_s3:
        current = _read_json_from_s3(registry_uri) or {"models": {}}
    else:
        if os.path.exists(registry_uri):
            with open(registry_uri, "r", encoding="utf-8") as f:
                current = json.load(f)
        else:
            current = {"models": {}}

    models = current.setdefault("models", {})
    history = models.setdefault(model_name, [])
    version = (history[-1]["version"] + 1) if history else 1

    entry = ModelRegistryEntry(
        model_name=model_name,
        version=version,
        run_id=run_id,
        metric_auc=float(metric_auc),
        model_uri=model_uri,
        created_at=_utc_now_iso(),
    )
    history.append(asdict(entry))

    if is_s3:
        _write_json_to_s3(registry_uri, current)
    else:
        os.makedirs(os.path.dirname(registry_uri) or ".", exist_ok=True)
        with open(registry_uri, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)

    return entry

