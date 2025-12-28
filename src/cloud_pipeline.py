from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import sys
import boto3
from datetime import datetime

# When using --py-files with Spark, the zip file is extracted and modules are available
# Add current directory to path as fallback
if '__file__' in globals():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)

# Import dependencies (will work with --py-files zip or if files are in same directory)
try:
    from entity_resolution import assign_corporate_ids
    print(f"[{datetime.now()}] ✅ Successfully imported entity_resolution")
except ImportError as e:
    print(f"[{datetime.now()}] ❌ Failed to import entity_resolution: {e}")
    print(f"[{datetime.now()}] 📂 Current sys.path: {sys.path}")
    print(f"[{datetime.now()}] 💡 Make sure entity_resolution.py is included in --py-files zip")
    raise

try:
    from ml_training import train_model
    print(f"[{datetime.now()}] ✅ Successfully imported ml_training")
except ImportError as e:
    print(f"[{datetime.now()}] ❌ Failed to import ml_training: {e}")
    print(f"[{datetime.now()}] 💡 Make sure ml_training.py is included in --py-files zip")
    raise

# -------------------------------------------------------------------
# Spark session (EMR-safe)
# Iceberg is configured at EMR cluster level
# -------------------------------------------------------------------
def create_cloud_spark_session():
    return (
        SparkSession.builder
        .appName("CorporateDataPipeline-Iceberg-Upsert")
        .getOrCreate()
    )

# -------------------------------------------------------------------
# Download raw data from S3 (READ ONLY)
# Uses EMR IAM role (no explicit credentials)
# -------------------------------------------------------------------
def download_s3_file(bucket, key, local_path):
    s3 = boto3.client("s3")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)

# -------------------------------------------------------------------
# Main pipeline
# -------------------------------------------------------------------
def run_cloud_pipeline():
    print(f"\n{'='*80}")
    print(f"[{datetime.now()}] 🚀 Starting Corporate Data Pipeline")
    print(f"{'='*80}\n")
    
    spark = create_cloud_spark_session()
    bucket = os.environ.get("S3_BUCKET")
    
    if not bucket:
        raise ValueError("S3_BUCKET environment variable is not set")
    
    print(f"[{datetime.now()}] 📦 S3 Bucket: {bucket}")

    # Iceberg warehouse (must match EMR config)
    warehouse = f"s3://{bucket}/iceberg/warehouse"
    print(f"[{datetime.now()}] 🏠 Iceberg Warehouse: {warehouse}")

    try:
        # ------------------------------------------------------------
        # 1. Download raw JSON files from S3 (READ ONLY)
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 📥 Step 1: Downloading data from S3...")
        print(f"[{datetime.now()}]   Downloading: s3://{bucket}/data/supply_chain_data.json")
        download_s3_file(
            bucket,
            "data/supply_chain_data.json",
            "/tmp/supply_chain_data.json"
        )
        print(f"[{datetime.now()}]   Downloading: s3://{bucket}/data/financial_data.json")
        download_s3_file(
            bucket,
            "data/financial_data.json",
            "/tmp/financial_data.json"
        )
        print(f"[{datetime.now()}] ✅ Data downloaded successfully")

        # ------------------------------------------------------------
        # 2. Load raw data into Spark
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 📊 Step 2: Loading data into Spark...")
        supply_df = spark.read.json("/tmp/supply_chain_data.json")
        supply_df = supply_df.withColumn(
            "supplier_count",
            size(col("top_suppliers"))
        )
        supply_count = supply_df.count()
        print(f"[{datetime.now()}]   Supply chain records: {supply_count}")

        financial_df = spark.read.json("/tmp/financial_data.json")
        financial_count = financial_df.count()
        print(f"[{datetime.now()}]   Financial records: {financial_count}")
        print(f"[{datetime.now()}] ✅ Data loaded successfully")
        
        # ------------------------------------------------------------
        # 3. Entity resolution
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🔍 Step 3: Performing entity resolution...")
        id_mapping = assign_corporate_ids(supply_df, financial_df)
        print(f"[{datetime.now()}]   Resolved {len(id_mapping)} corporate entities")
        print(f"[{datetime.now()}] ✅ Entity resolution completed")

        # ------------------------------------------------------------
        # 4. Harmonization (assignment logic kept unchanged)
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🔄 Step 4: Harmonizing data...")
        harmonized_records = []

        supply_pandas = supply_df.toPandas()
        for idx, row in supply_pandas.iterrows():
            harmonized_records.append({
                "corporate_id": str(id_mapping[f"s1_{idx}"]),  # Convert to string to match table schema
                "corporate_name": row.get("corporate_name_S1"),
                "address": row.get("address"),
                "supplier_count": row.get("supplier_count", 0),
                "revenue": None,
                "profit": None
            })

        financial_pandas = financial_df.toPandas()
        for idx, row in financial_pandas.iterrows():
            corporate_id = str(id_mapping[f"s2_{idx}"])  # Convert to string to match table schema
            existing = next(
                (r for r in harmonized_records if r["corporate_id"] == corporate_id),
                None
            )

            if existing:
                existing["revenue"] = row.get("revenue")
                existing["profit"] = row.get("profit")
            else:
                harmonized_records.append({
                    "corporate_id": corporate_id,
                    "corporate_name": row.get("corporate_name_S2"),
                    "address": None,
                    "supplier_count": 0,
                    "revenue": row.get("revenue"),
                    "profit": row.get("profit")
                })

        # Define schema explicitly to match Iceberg table schema
        schema = StructType([
            StructField("corporate_id", StringType(), True),
            StructField("corporate_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("supplier_count", IntegerType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("profit", DoubleType(), True)
        ])
        
        harmonized_df = spark.createDataFrame(harmonized_records, schema=schema)
        harmonized_count = harmonized_df.count()
        print(f"[{datetime.now()}]   Created {harmonized_count} harmonized records")
        print(f"[{datetime.now()}] ✅ Data harmonization completed")

        # ------------------------------------------------------------
        # 5. Iceberg database & target table
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🗄️  Step 5: Setting up Iceberg database and tables...")
        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS glue_catalog.corporate_db
            LOCATION '{warehouse}/corporate_db'
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
        print(f"[{datetime.now()}]   Created/verified main table: corporate_registry")

        # ------------------------------------------------------------
        # 6. Write to staging Iceberg table
        # Ensure staging table has the same schema as main table
        # Drop and recreate staging table to ensure clean state and correct schema
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 📝 Step 6: Writing to staging table...")
        spark.sql("""
            DROP TABLE IF EXISTS glue_catalog.corporate_db.corporate_registry_staging
        """)
        print(f"[{datetime.now()}]   Dropped existing staging table (if any)")
        
        # Create staging table with explicit schema matching main table
        spark.sql("""
            CREATE TABLE glue_catalog.corporate_db.corporate_registry_staging (
                corporate_id STRING,
                corporate_name STRING,
                address STRING,
                supplier_count INT,
                revenue DOUBLE,
                profit DOUBLE
            )
            USING iceberg
        """)
        print(f"[{datetime.now()}]   Created staging table")
        
        # Write data to staging table
        harmonized_df.write.format("iceberg").mode("append").saveAsTable(
            "glue_catalog.corporate_db.corporate_registry_staging"
        )
        staging_count = spark.sql("SELECT COUNT(*) as cnt FROM glue_catalog.corporate_db.corporate_registry_staging").collect()[0]['cnt']
        print(f"[{datetime.now()}]   Wrote {staging_count} records to staging table")
        print(f"[{datetime.now()}] ✅ Staging table write completed")

        # ------------------------------------------------------------
        # 7. UPSERT into main Iceberg table (MERGE INTO)
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🔀 Step 7: Performing UPSERT (MERGE INTO) to main table...")
        before_count = spark.sql("SELECT COUNT(*) as cnt FROM glue_catalog.corporate_db.corporate_registry").collect()[0]['cnt']
        print(f"[{datetime.now()}]   Records in main table before merge: {before_count}")
        
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
        
        after_count = spark.sql("SELECT COUNT(*) as cnt FROM glue_catalog.corporate_db.corporate_registry").collect()[0]['cnt']
        print(f"[{datetime.now()}]   Records in main table after merge: {after_count}")
        print(f"[{datetime.now()}] ✅ UPSERT completed successfully")

        # ------------------------------------------------------------
        # 8. Drop staging table (clean-up)
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🧹 Step 8: Cleaning up staging table...")
        spark.sql("""
            DROP TABLE glue_catalog.corporate_db.corporate_registry_staging
        """)
        print(f"[{datetime.now()}] ✅ Staging table dropped")

        # ------------------------------------------------------------
        # 9. Train ML model (unchanged)
        # ------------------------------------------------------------
        print(f"\n[{datetime.now()}] 🤖 Step 9: Training ML model...")
        train_model(harmonized_df)
        print(f"[{datetime.now()}] ✅ ML model training completed")

        print(f"\n{'='*80}")
        print(f"[{datetime.now()}] ✅ EMR Iceberg pipeline completed successfully!")
        print(f"{'='*80}\n")
        
        print(f"[{datetime.now()}] 📊 Final harmonized data preview:")
        harmonized_df.show(truncate=False, numRows=20)

    except Exception as e:
        print(f"\n{'='*80}")
        print(f"[{datetime.now()}] ❌ ERROR: Pipeline failed with exception")
        print(f"[{datetime.now()}] Error type: {type(e).__name__}")
        print(f"[{datetime.now()}] Error message: {str(e)}")
        print(f"{'='*80}\n")
        import traceback
        traceback.print_exc()
        raise
    finally:
        print(f"[{datetime.now()}] 🛑 Stopping Spark session...")
        spark.stop()
        print(f"[{datetime.now()}] ✅ Spark session stopped")

# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    run_cloud_pipeline()