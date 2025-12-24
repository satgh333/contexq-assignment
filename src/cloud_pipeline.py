from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size
from src.entity_resolution import assign_corporate_ids
from src.ml_training import train_model
import os
import boto3

def create_cloud_spark_session():
    """Create Spark session for cloud execution"""
    spark = SparkSession.builder \
        .appName("CorporateDataPipeline-Cloud") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    return spark

def download_s3_file(bucket, key, local_path):
    """Download file from S3 using boto3"""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1"
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3.download_file(bucket, key, local_path)

def run_cloud_pipeline():
    """Run pipeline with S3 data sources"""
    spark = create_cloud_spark_session()
    bucket = os.environ["S3_BUCKET"]
    
    try:
        # Download JSON files from S3
        download_s3_file(bucket, "data/supply_chain_data.json", "/tmp/supply_chain_data.json")
        download_s3_file(bucket, "data/financial_data.json", "/tmp/financial_data.json")
        
        # Load data into Spark
        supply_df = spark.read.json("/tmp/supply_chain_data.json")
        supply_df = supply_df.withColumn("supplier_count", size(col("top_suppliers")))
        
        financial_df = spark.read.json("/tmp/financial_data.json")
        
        # Entity resolution
        id_mapping = assign_corporate_ids(supply_df, financial_df)
        
        # Harmonized dataset
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
            existing_record = next((r for r in harmonized_records if r["corporate_id"] == corporate_id), None)
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
        
        # Save harmonized data locally and upload to S3
        harmonized_df.write.mode("overwrite").parquet("/tmp/harmonized_data")
        
        # Upload output back to S3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name="us-east-1"
        )
        import glob
        for file_path in glob.glob("/tmp/harmonized_data/*"):
            s3.upload_file(file_path, bucket, f"output/harmonized_data/{os.path.basename(file_path)}")
        
        # Train ML model
        train_model(harmonized_df)
        
        print("Cloud pipeline completed successfully!")
        harmonized_df.show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_cloud_pipeline()
