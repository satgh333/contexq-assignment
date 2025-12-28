# jm_requirement
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, size
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from src.entity_resolution import assign_corporate_ids
from src.ml_training import train_model
import json

def create_local_spark_session():
    """Create Spark session for local testing"""
    return SparkSession.builder \
        .appName("CorporateDataPipeline-Local") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def run_local_pipeline():
    """Run pipeline locally without Iceberg"""
    spark = create_local_spark_session()
    
    try:
        # Load data (now in single-line JSON format)
        supply_df = spark.read.json("data/supply_chain_data.json")
        supply_df = supply_df.withColumn("supplier_count", size(col("top_suppliers")))
        
        financial_df = spark.read.json("data/financial_data.json")
        
        # Entity resolution
        id_mapping = assign_corporate_ids(supply_df, financial_df)
        
        # Create harmonized dataset
        harmonized_records = []
        
        # Process supply chain data
        supply_pandas = supply_df.toPandas()
        for idx, row in supply_pandas.iterrows():
            corporate_id = id_mapping[f"s1_{idx}"]
            harmonized_records.append({
                "corporate_id": corporate_id,
                "corporate_name": row["corporate_name_S1"],
                "address": row["address"],
                "supplier_count": row["supplier_count"],
                "revenue": None,
                "profit": None
            })
        
        # Process financial data
        financial_pandas = financial_df.toPandas()
        for idx, row in financial_pandas.iterrows():
            corporate_id = id_mapping[f"s2_{idx}"]
            
            # Update existing or create new
            existing_record = None
            for record in harmonized_records:
                if record["corporate_id"] == corporate_id:
                    existing_record = record
                    break
            
            if existing_record:
                existing_record["revenue"] = row["revenue"]
                existing_record["profit"] = row["profit"]
            else:
                harmonized_records.append({
                    "corporate_id": corporate_id,
                    "corporate_name": row["corporate_name_S2"],
                    "address": None,
                    "supplier_count": 0,
                    "revenue": row["revenue"],
                    "profit": row["profit"]
                })
        
        # Define schema explicitly for consistency
        schema = StructType([
            StructField("corporate_id", LongType(), True),
            StructField("corporate_name", StringType(), True),
            StructField("address", StringType(), True),
            StructField("supplier_count", IntegerType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("profit", DoubleType(), True)
        ])
        
        harmonized_df = spark.createDataFrame(harmonized_records, schema=schema)
        
        # Save locally
        harmonized_df.write.mode("overwrite").parquet("output/harmonized_data")
        
        # Train ML model
        train_model(harmonized_df)
        
        print("Local pipeline completed successfully!")
        harmonized_df.show()
        
    finally:
        spark.stop()

if __name__ == "__main__":
    run_local_pipeline()
