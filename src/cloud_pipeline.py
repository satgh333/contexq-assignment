from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
from src.entity_resolution import assign_corporate_ids
from src.ml_training import train_model
import os
import boto3


# -------------------------------------------------------------------
# Spark session with Iceberg support
# -------------------------------------------------------------------
def create_cloud_spark_session():
    spark = (
        SparkSession.builder
        .appName("CorporateDataPipeline-Iceberg-Upsert")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            "spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog"
        )
        .config(
            "spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.sql.catalog.glue_catalog.warehouse",
            "s3://YOUR-BUCKET/iceberg/warehouse/"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark


# -------------------------------------------------------------------
# Download raw data from S3 (READ ONLY)
# -------------------------------------------------------------------
def download_s3_file(bucket, key, local_path):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1"
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)


# -------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------
def run_cloud_pipeline():
    spark = create_cloud_spark_session()
    bucket = os.environ["S3_BUCKET"]

    try:
        # ------------------------------------------------------------
        # 1. Download raw JSON files from S3 (READ ONLY)
        # ------------------------------------------------------------
        download_s3_file(bucket, "data/supply_chain_data.json", "/tmp/supply_chain_data.json")
        download_s3_file(bucket, "data/financial_data.json", "/tmp/financial_data.json")

        # ------------------------------------------------------------
        # 2. Load raw data into Spark
        # ------------------------------------------------------------
        supply_df = spark.read.json("/tmp/supply_chain_data.json")
        supply_df = supply_df.withColumn(
            "supplier_count",
            size(col("top_suppliers"))
        )

        financial_df = spark.read.json("/tmp/financial_data.json")

        # ------------------------------------------------------------
        # 3. Entity resolution
        # ------------------------------------------------------------
        id_mapping = assign_corporate_ids(supply_df, financial_df)

        # ------------------------------------------------------------
        # 4. Harmonization (UNCHANGED LOGIC)
        # ------------------------------------------------------------
        harmonized_records = []

        supply_pandas = supply_df.toPandas()
        for idx, row in supply_pandas.iterrows():
            corporate_id = id_mapping[f"s1_{idx}"]
            harmonized_records.append({
                "corporate_id": corporate_id,
                "corporate_name": row.get("corporate_name_S1"),
                "address": row.get("address"),
                "supplier_count": row.get("supplier_count", 0),
                "revenue": None,
                "profit": None
            })

        financial_pandas = financial_df.toPandas()
        for idx, row in financial_pandas.iterrows():
            corporate_id = id_mapping[f"s2_{idx}"]
            existing_record = next(
                (r for r in harmonized_records if r["corporate_id"] == corporate_id),
                None
            )

            if existing_record:
                existing_record["revenue"] = row.get("revenue")
                existing_record["profit"] = row.get("profit")
            else:
                harmonized_records.append({
                    "corporate_id": corporate_id,
                    "corporate_name": row.get("corporate_name_S2"),
                    "address": None,
                    "supplier_count": 0,
                    "revenue": row.get("revenue"),
                    "profit": row.get("profit")
                })

        harmonized_df = spark.createDataFrame(harmonized_records)

        # ------------------------------------------------------------
        # 5. ICEBERG SETUP
        # ------------------------------------------------------------
        spark.sql("""
            CREATE DATABASE IF NOT EXISTS glue_catalog.corporate_db
            LOCATION 's3://YOUR-BUCKET/iceberg/warehouse/corporate_db'
        """)

        spark.sql("""
            CREATE TABLE IF NOT EXISTS glue_catalog.corporate_db.corporate_registry (
                corporate_id STRING,
                corporate_name STRING,
                address STRING,
                supplier_count INT,
                revenue DOUBLE,
                profit DOUBLE
            )
            USING iceberg
        """)

        # ------------------------------------------------------------
        # 6. WRITE TO STAGING TABLE
        # ------------------------------------------------------------
        harmonized_df.writeTo(
            "glue_catalog.corporate_db.corporate_registry_staging"
        ).using("iceberg").createOrReplace()

        # ------------------------------------------------------------
        # 7. UPSERT (MERGE INTO) – CORE REQUIREMENT
        # ------------------------------------------------------------
        spark.sql("""
        MERGE INTO glue_catalog.corporate_db.corporate_registry t
        USING glue_catalog.corporate_db.corporate_registry_staging s
        ON t.corporate_id = s.corporate_id

        WHEN MATCHED THEN UPDATE SET
          t.corporate_name = COALESCE(s.corporate_name, t.corporate_name),
          t.address        = COALESCE(s.address, t.address),
          t.supplier_count = GREATEST(t.supplier_count, s.supplier_count),
          t.revenue        = COALESCE(s.revenue, t.revenue),
          t.profit         = COALESCE(s.profit, t.profit)

        WHEN NOT MATCHED THEN INSERT (
          corporate_id,
          corporate_name,
          address,
          supplier_count,
          revenue,
          profit
        )
        VALUES (
          s.corporate_id,
          s.corporate_name,
          s.address,
          s.supplier_count,
          s.revenue,
          s.profit
        )
        """)

        # ------------------------------------------------------------
        # 8. CLEAN UP STAGING TABLE (OPTIONAL BUT CLEAN)
        # ------------------------------------------------------------
        spark.sql("""
            DROP TABLE glue_catalog.corporate_db.corporate_registry_staging
        """)

        # ------------------------------------------------------------
        # 9. Train ML model (UNCHANGED)
        # ------------------------------------------------------------
        train_model(harmonized_df)

        print("Pipeline completed successfully with Iceberg MERGE ✅")
        harmonized_df.show()

    finally:
        spark.stop()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    run_cloud_pipeline()
