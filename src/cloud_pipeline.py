# Add immediate output to verify script is running - MUST be first
import sys
import os

# Force output to stderr (which goes to Spark logs)
def log(msg):
    print(msg, file=sys.stderr)
    sys.stderr.flush()

log("=" * 80)
log("🚀 cloud_pipeline.py: Script started")
log(f"Python executable: {sys.executable}")
log(f"Python version: {sys.version}")
log(f"Current working directory: {os.getcwd()}")
log("=" * 80)

# Check environment variables early
log("🔍 Checking environment variables...")
s3_bucket = os.environ.get("S3_BUCKET")
if s3_bucket:
    log(f"✅ S3_BUCKET is set: {s3_bucket}")
else:
    log("⚠️  S3_BUCKET is NOT set (will fail later if not provided)")

log("📦 Starting imports...")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, size
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    import boto3
    from datetime import datetime
    log("✅ Core imports successful")
except Exception as e:
    log(f"❌ Core imports failed: {e}")
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise

# When using --py-files with Spark, the zip file is extracted and modules are available
# Add current directory and common temp locations to path
log("📂 Setting up Python path for imports...")
if '__file__' in globals():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
        log(f"Added script directory to path: {script_dir}")

# Add common temp locations where Spark extracts --py-files
for path in ['/tmp', '/mnt/tmp', os.path.expanduser('~'), '/var/task']:
    if path not in sys.path and os.path.exists(path):
        sys.path.insert(0, path)

# Try to find where Spark extracted the zip file by looking for our modules
log("🔍 Searching for extracted zip file location...")
import glob
for search_path in sys.path:
    if os.path.exists(search_path):
        # Look for entity_resolution.py in this path
        entity_file = os.path.join(search_path, "entity_resolution.py")
        if os.path.exists(entity_file):
            log(f"✅ Found entity_resolution.py at: {entity_file}")
            if search_path not in sys.path:
                sys.path.insert(0, search_path)
            break
        # Also check subdirectories
        for root, dirs, files in os.walk(search_path):
            if "entity_resolution.py" in files:
                log(f"✅ Found entity_resolution.py in subdirectory: {root}")
                if root not in sys.path:
                    sys.path.insert(0, root)
                break

log(f"📂 Final sys.path (first 8): {sys.path[:8]}")

# Import dependencies (will work with --py-files zip or if files are in same directory)
log("🔍 Attempting to import entity_resolution...")

try:
    from entity_resolution import assign_corporate_ids
    log("✅ Successfully imported entity_resolution")
except ImportError as e:
    log(f"❌ Failed to import entity_resolution: {e}")
    log(f"📂 Full sys.path: {sys.path}")
    log("💡 Trying to find entity_resolution.py manually...")
    
    # Try to find the file manually
    import glob
    for path in sys.path:
        potential_file = os.path.join(path, "entity_resolution.py")
        if os.path.exists(potential_file):
            log(f"✅ Found entity_resolution.py at: {potential_file}")
            break
    else:
        log("❌ entity_resolution.py not found in any sys.path location")
    
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise

log("🔍 Attempting to import ml_training...")

try:
    from ml_training import train_model
    log("✅ Successfully imported ml_training")
except ImportError as e:
    log(f"❌ Failed to import ml_training: {e}")
    log("💡 Trying to find ml_training.py manually...")
    
    # Try to find the file manually
    import glob
    for path in sys.path:
        potential_file = os.path.join(path, "ml_training.py")
        if os.path.exists(potential_file):
            log(f"✅ Found ml_training.py at: {potential_file}")
            break
    else:
        log("❌ ml_training.py not found in any sys.path location")
    
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise

log("=" * 80)
log("✅ All imports successful - ready to run pipeline")
log("=" * 80)

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
    log(f"\n{'='*80}")
    log(f"[{datetime.now()}] 🚀 Starting Corporate Data Pipeline")
    log(f"{'='*80}\n")
    
    # Check S3_BUCKET early
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        error_msg = "S3_BUCKET environment variable is not set. Please set it using --conf spark.yarn.appMasterEnv.S3_BUCKET=<bucket>"
        log(f"❌ FATAL ERROR: {error_msg}")
        log("💡 Available environment variables:")
        for key, value in os.environ.items():
            if 'S3' in key or 'BUCKET' in key:
                log(f"   {key}={value}")
        raise ValueError(error_msg)
    
    log(f"[{datetime.now()}] 📦 S3 Bucket: {bucket}")
    
    spark = create_cloud_spark_session()
    log(f"[{datetime.now()}] ✅ Spark session created")

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
    try:
        log("=" * 80)
        log("🎬 ENTRY POINT: Starting main execution")
        log("=" * 80)
        run_cloud_pipeline()
        log("=" * 80)
        log("✅ Pipeline completed successfully")
        log("=" * 80)
    except KeyboardInterrupt:
        log("\n⚠️  Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        log(f"\n{'='*80}")
        log(f"❌ FATAL ERROR: Pipeline failed")
        log(f"Error type: {type(e).__name__}")
        log(f"Error message: {str(e)}")
        log(f"{'='*80}")
        import traceback
        log("Full traceback:")
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        sys.exit(1)