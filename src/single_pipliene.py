from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
import os
import boto3
import re
import Levenshtein
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
import mlflow
import mlflow.spark



def engineer_features(df: DataFrame) -> DataFrame:
    df_features = df.withColumn("high_profit", when(col("profit") > 500000, 1).otherwise(0))
    df_features = df_features.fillna({
        "revenue": 0,
        "profit": 0,
        "supplier_count": 0
    })
    return df_features

def train_model(df: DataFrame) -> None:
    with mlflow.start_run():
        df_ml = engineer_features(df)
        assembler = VectorAssembler(inputCols=["revenue", "supplier_count"], outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="high_profit", maxIter=10)
        pipeline = Pipeline(stages=[assembler, lr])
        train_df, test_df = df_ml.randomSplit([0.8, 0.2], seed=42)
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol="high_profit")
        auc = evaluator.evaluate(predictions)
        mlflow.log_metric("auc", auc)
        mlflow.spark.log_model(model, "corporate_profit_model")
        print(f"Model trained with AUC: {auc}")
        return model
    
# -------------------------------------------------------------------
# Spark session (EMR-safe)
# Iceberg is configured at EMR cluster level
# -------------------------------------------------------------------
def clean_corporate_name(name: str) -> str:
    """Clean corporate name for matching"""
    if not name:
        return ""
    # Remove punctuation, normalize case, remove common suffixes
    cleaned = re.sub(r'[^\w\s]', '', name.lower())
    cleaned = re.sub(r'\b(inc|corp|ltd|llc|company|co)\b', '', cleaned)
    return ' '.join(cleaned.split())

def simplify_address(address: str) -> str:
    """Extract city/state from address"""
    if not address:
        return ""
    # Simple extraction - in real scenario would use address parsing library
    parts = address.split(',')
    if len(parts) >= 2:
        return f"{parts[-2].strip()} {parts[-1].strip()}".lower()  # ✅ force lowercase
    return address.lower().strip()

def calculate_similarity(name1: str, addr1: str, name2: str, addr2: str) -> float:
    """Calculate similarity score between two corporate records"""
    name_sim = Levenshtein.ratio(clean_corporate_name(name1), clean_corporate_name(name2))
    addr_sim = Levenshtein.ratio(simplify_address(addr1), simplify_address(addr2))
    return (name_sim * 0.7) + (addr_sim * 0.3)  # Weight name more heavily

def assign_corporate_ids(df1: DataFrame, df2: DataFrame, threshold: float = 0.85) -> dict:
    """Assign unique corporate IDs based on entity resolution"""
    # Convert to Pandas for easier processing (in production, use distributed approach)
    pdf1 = df1.toPandas()
    pdf2 = df2.toPandas()
    
    corporate_id = 1
    id_mapping = {}
    
    # Process source 1
    for idx, row in pdf1.iterrows():
        key = f"s1_{idx}"
        id_mapping[key] = corporate_id
        corporate_id += 1
    
    # Process source 2 with matching
    for idx, row in pdf2.iterrows():
        best_match_id = None
        best_score = 0
        
        for s1_idx, s1_row in pdf1.iterrows():
            score = calculate_similarity(
                row['corporate_name_S2'], row.get('address', ''),
                s1_row['corporate_name_S1'], s1_row.get('address', '')
            )
            if score > threshold and score > best_score:
                best_score = score
                best_match_id = id_mapping[f"s1_{s1_idx}"]
        
        if best_match_id:
            id_mapping[f"s2_{idx}"] = best_match_id
        else:
            id_mapping[f"s2_{idx}"] = corporate_id
            corporate_id += 1
    
    return id_mapping

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
    spark = create_cloud_spark_session()
    bucket = os.environ["S3_BUCKET"]

    # Iceberg warehouse (must match EMR config)
    warehouse = f"s3://{bucket}/iceberg/warehouse"

    try:
        # ------------------------------------------------------------
        # 1. Download raw JSON files from S3 (READ ONLY)
        # ------------------------------------------------------------
        download_s3_file(
            bucket,
            "data/supply_chain_data.json",
            "/tmp/supply_chain_data.json"
        )
        download_s3_file(
            bucket,
            "data/financial_data.json",
            "/tmp/financial_data.json"
        )
        print("downloaded the data")

        # ------------------------------------------------------------
        # 2. Load raw data into Spark
        # ------------------------------------------------------------
        supply_df = spark.read.json("/tmp/supply_chain_data.json")
        supply_df = supply_df.withColumn(
            "supplier_count",
            size(col("top_suppliers"))
        )

        financial_df = spark.read.json("/tmp/financial_data.json")

        print("Count for supply chain =",supply_df.count())
        print("Count for financial_df =",financial_df.count())
        
        # ------------------------------------------------------------
        # 3. Entity resolution
        # ------------------------------------------------------------
        id_mapping = assign_corporate_ids(supply_df, financial_df)

        # ------------------------------------------------------------
        # 4. Harmonization (assignment logic kept unchanged)
        # ------------------------------------------------------------
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

        # ------------------------------------------------------------
        # 5. Iceberg database & target table
        # ------------------------------------------------------------
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

        # ------------------------------------------------------------
        # 6. Write to staging Iceberg table
        # Ensure staging table has the same schema as main table
        # Drop and recreate staging table to ensure clean state and correct schema
        # ------------------------------------------------------------
        spark.sql("""
            DROP TABLE IF EXISTS glue_catalog.corporate_db.corporate_registry_staging
        """)
        
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
        
        # Write data to staging table
        harmonized_df.write.format("iceberg").mode("append").saveAsTable(
            "glue_catalog.corporate_db.corporate_registry_staging"
        )

        # ------------------------------------------------------------
        # 7. UPSERT into main Iceberg table (MERGE INTO)
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
        # 8. Drop staging table (clean-up)
        # ------------------------------------------------------------
        spark.sql("""
            DROP TABLE glue_catalog.corporate_db.corporate_registry_staging
        """)

        # ------------------------------------------------------------
        # 9. Train ML model (unchanged)
        # ------------------------------------------------------------
        train_model(harmonized_df)

        print("✅ EMR Iceberg pipeline completed successfully")
        harmonized_df.show(truncate=False)

    finally:
        spark.stop()

# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------
if __name__ == "__main__":
    run_cloud_pipeline()