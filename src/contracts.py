from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)


CORPORATE_REGISTRY_EXPECTED_TYPES = {
    "corporate_id": LongType(),
    "corporate_name": StringType(),
    "address": StringType(),
    "activity_places": ArrayType(StringType()),
    "top_suppliers": ArrayType(StringType()),
    "supplier_count": IntegerType(),
    "revenue": DoubleType(),
    "profit": DoubleType(),
    "main_customers": ArrayType(StringType()),
    "updated_at": TimestampType(),
}


def validate_corporate_registry_df(df: DataFrame) -> None:
    """
    Data contract check: ensure a harmonized DataFrame conforms to the expected
    Iceberg `corporate_registry` schema (column set + Spark data types).
    """
    schema = df.schema
    actual = {f.name: f.dataType for f in schema.fields}

    missing = [c for c in CORPORATE_REGISTRY_EXPECTED_TYPES.keys() if c not in actual]
    if missing:
        raise ValueError(f"Data contract failed: missing columns: {missing}")

    mismatched = []
    for col_name, expected_type in CORPORATE_REGISTRY_EXPECTED_TYPES.items():
        if actual[col_name] != expected_type:
            mismatched.append((col_name, str(expected_type), str(actual[col_name])))

    if mismatched:
        raise ValueError(f"Data contract failed: type mismatches: {mismatched}")

