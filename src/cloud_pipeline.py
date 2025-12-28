"""
Thin wrapper for running the pipeline on EMR / cloud environments.

Expected inputs are read directly from S3 via `s3a://...` (Spark can parallelize reads).

Environment variables (recommended):
- S3_BUCKET: bucket containing your source data + warehouse
- SOURCE1_URI: e.g. s3a://<bucket>/sources/supply_chain/*.json
- SOURCE2_URI: e.g. s3a://<bucket>/sources/financial/*.json
- ICEBERG_CATALOG_NAME: e.g. glue_catalog
- ICEBERG_CATALOG_TYPE: glue
- ICEBERG_WAREHOUSE: s3://<bucket>/warehouse
- MODEL_REGISTRY_URI: s3://<bucket>/models/model_registry.json

On EMR, set ENABLE_SPARK_PACKAGES=0 so cluster-provided Iceberg/S3 jars are used.
"""

from __future__ import annotations

import os

from src.etl_pipeline import main


def _set_default_env(key: str, value: str) -> None:
    if not os.getenv(key):
        os.environ[key] = value


if __name__ == "__main__":
    bucket = os.getenv("S3_BUCKET")
    if bucket:
        _set_default_env("SOURCE1_URI", f"s3a://{bucket}/data/supply_chain_data.json")
        _set_default_env("SOURCE2_URI", f"s3a://{bucket}/data/financial_data.json")
        _set_default_env("ICEBERG_WAREHOUSE", f"s3://{bucket}/warehouse")
        _set_default_env("MODEL_REGISTRY_URI", f"s3://{bucket}/models/model_registry.json")

    _set_default_env("ICEBERG_CATALOG_NAME", "glue_catalog")
    _set_default_env("ICEBERG_CATALOG_TYPE", "glue")
    _set_default_env("ICEBERG_DATABASE", "corporate_data")
    _set_default_env("ICEBERG_TABLE", "corporate_registry")
    _set_default_env("ENABLE_SPARK_PACKAGES", "0")

    main()