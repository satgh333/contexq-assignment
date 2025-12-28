from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.contracts import validate_corporate_registry_df


def test_validate_corporate_registry_df_ok():
    spark = SparkSession.builder.master("local[1]").appName("test-contracts").getOrCreate()
    try:
        schema = StructType(
            [
                StructField("corporate_id", LongType(), True),
                StructField("corporate_name", StringType(), True),
                StructField("address", StringType(), True),
                StructField("activity_places", ArrayType(StringType()), True),
                StructField("top_suppliers", ArrayType(StringType()), True),
                StructField("supplier_count", IntegerType(), True),
                StructField("revenue", DoubleType(), True),
                StructField("profit", DoubleType(), True),
                StructField("main_customers", ArrayType(StringType()), True),
                StructField("updated_at", TimestampType(), True),
            ]
        )
        df = spark.createDataFrame([], schema=schema)
        validate_corporate_registry_df(df)
    finally:
        spark.stop()


def test_validate_corporate_registry_df_missing_column():
    spark = SparkSession.builder.master("local[1]").appName("test-contracts").getOrCreate()
    try:
        schema = StructType([StructField("corporate_id", LongType(), True)])
        df = spark.createDataFrame([], schema=schema)
        try:
            validate_corporate_registry_df(df)
            assert False, "expected ValueError"
        except ValueError as e:
            assert "missing columns" in str(e)
    finally:
        spark.stop()

