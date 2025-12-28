# jm_requirement
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, size, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyiceberg.catalog import load_catalog
from entity_resolution import assign_corporate_ids
from ml_training import train_model
import json

def create_spark_session():
    """Create Spark session with Iceberg support"""
    return SparkSession.builder \
        .appName("CorporateDataPipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{os.getenv('S3_BUCKET')}/warehouse") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .getOrCreate()

def load_source_data(spark):
    """Load data from both sources"""
    # Load supply chain data
    supply_df = spark.read.json("data/supply_chain_data.json")
    supply_df = supply_df.withColumn("supplier_count", size(col("top_suppliers")))
    
    # Load financial data  
    financial_df = spark.read.json("data/financial_data.json")
    
    return supply_df, financial_df

def harmonize_data(spark, supply_df, financial_df):
    """Perform entity resolution and harmonize data"""
    # Get corporate ID mappings
    id_mapping = assign_corporate_ids(supply_df, financial_df)
    
    # Create harmonized dataset
    harmonized_records = []
    
    # Process supply chain data
    supply_pandas = supply_df.toPandas()
    for idx, row in supply_pandas.iterrows():
        corporate_id = id_mapping[f"s1_{idx}"]  # Keep as integer for BIGINT table
        harmonized_records.append({
            "corporate_id": corporate_id,
            "corporate_name": row["corporate_name_S1"],
            "address": row["address"],
            "activity_places": row["activity_places"],
            "supplier_count": row["supplier_count"],
            "revenue": None,
            "profit": None,
            "main_customers": None
        })
    
    # Process financial data
    financial_pandas = financial_df.toPandas()
    for idx, row in financial_pandas.iterrows():
        corporate_id = id_mapping[f"s2_{idx}"]  # Keep as integer for BIGINT table
        
        # Check if this corporate_id already exists
        existing_record = None
        for record in harmonized_records:
            if record["corporate_id"] == corporate_id:
                existing_record = record
                break
        
        if existing_record:
            # Update existing record
            existing_record["revenue"] = row["revenue"]
            existing_record["profit"] = row["profit"]
            existing_record["main_customers"] = row["main_customers"]
        else:
            # Create new record
            harmonized_records.append({
                "corporate_id": corporate_id,
                "corporate_name": row["corporate_name_S2"],
                "address": None,
                "activity_places": None,
                "supplier_count": 0,
                "revenue": row["revenue"],
                "profit": row["profit"],
                "main_customers": row["main_customers"]
            })
    
    # Define schema explicitly to match Iceberg table schema (BIGINT for corporate_id)
    schema = StructType([
        StructField("corporate_id", LongType(), True),
        StructField("corporate_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("activity_places", StringType(), True),
        StructField("supplier_count", IntegerType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("profit", DoubleType(), True),
        StructField("main_customers", StringType(), True)
    ])
    
    return spark.createDataFrame(harmonized_records, schema=schema)

def upsert_to_iceberg(spark, harmonized_df):
    """Upsert data to Iceberg table"""
    # Create table if not exists
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.corporate_data.corporate_registry (
            corporate_id BIGINT,
            corporate_name STRING,
            address STRING,
            activity_places STRING,
            supplier_count INT,
            revenue DOUBLE,
            profit DOUBLE,
            main_customers STRING
        ) USING ICEBERG
        PARTITIONED BY (bucket(10, corporate_id))
    """)
    
    # Create temp view for merge
    harmonized_df.createOrReplaceTempView("updates")
    
    # Perform merge operation
    spark.sql("""
        MERGE INTO glue_catalog.corporate_data.corporate_registry t
        USING updates s ON t.corporate_id = s.corporate_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

def main():
    """Main pipeline execution"""
    spark = create_spark_session()
    
    try:
        # Load source data
        supply_df, financial_df = load_source_data(spark)
        
        # Harmonize data
        harmonized_df = harmonize_data(spark, supply_df, financial_df)
        
        # Upsert to Iceberg
        upsert_to_iceberg(spark, harmonized_df)
        
        # Read from Iceberg for ML training
        iceberg_df = spark.sql("SELECT * FROM glue_catalog.corporate_data.corporate_registry")
        
        # Train ML model
        train_model(iceberg_df)
        
        print("Pipeline completed successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
