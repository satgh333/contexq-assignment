import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, size
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

from src.entity_resolution import assign_corporate_ids
from src.ml_training import train_model
from src.contracts import validate_corporate_registry_df


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    return value if value not in ("", None) else default


def create_spark_session() -> SparkSession:
    """
    Create a Spark session with:
    - Iceberg Spark SQL extensions
    - Optional AWS Glue catalog (for EMR) OR local Hadoop catalog (for local/dev)
    - S3A filesystem for reading input sources from S3
    """
    catalog_name = _env("ICEBERG_CATALOG_NAME", "local")
    catalog_type = _env("ICEBERG_CATALOG_TYPE", "hadoop")  # hadoop | glue
    warehouse = _env("ICEBERG_WAREHOUSE", "file:/tmp/iceberg/warehouse")

    enable_packages = _env("ENABLE_SPARK_PACKAGES", "1") == "1"
    jars_packages = _env(
        "SPARK_JARS_PACKAGES",
        ",".join(
            [
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
                "org.apache.hadoop:hadoop-aws:3.3.6",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]
        ),
    )

    builder = (
        SparkSession.builder.appName("CorporateDataPipeline")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # S3A config (uses env/instance profile/role by default)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    # Pull runtime jars when running locally/CI. On EMR, prefer cluster-provided jars.
    if enable_packages and jars_packages:
        builder = builder.config("spark.jars.packages", jars_packages)

    if catalog_type == "glue":
        # AWS Glue catalog + S3 warehouse
        builder = (
            builder.config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
            .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        )
    else:
        # Local Hadoop catalog (lightweight metastore) - good for dev/CI
        builder = (
            builder.config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop")
            .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse)
        )

    return builder.getOrCreate()

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

def load_source_data(spark: SparkSession):
    """
    Load sources from S3 (preferred) or local files.

    Set:
    - SOURCE1_URI: e.g. s3a://my-bucket/sources/supply_chain/*.json
    - SOURCE2_URI: e.g. s3a://my-bucket/sources/financial/*.json
    """
    source1_uri = _env("SOURCE1_URI", "data/supply_chain_data.json")
    source2_uri = _env("SOURCE2_URI", "data/financial_data.json")

    supply_df = spark.read.json(source1_uri)
    supply_df = supply_df.withColumn("supplier_count", size(col("top_suppliers")))

    financial_df = spark.read.json(source2_uri)

    return supply_df, financial_df

def harmonize_data(spark: SparkSession, supply_df, financial_df):
    """Perform entity resolution and harmonize into a unified corporate registry."""
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
            "top_suppliers": row.get("top_suppliers"),
            "supplier_count": row["supplier_count"],
            "revenue": None,
            "profit": None,
            "main_customers": None,
            "updated_at": None,
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
            existing_record["main_customers"] = row.get("main_customers")
        else:
            # Create new record
            harmonized_records.append({
                "corporate_id": corporate_id,
                "corporate_name": row["corporate_name_S2"],
                "address": None,
                "activity_places": None,
                "top_suppliers": None,
                "supplier_count": 0,
                "revenue": row["revenue"],
                "profit": row["profit"],
                "main_customers": row.get("main_customers"),
                "updated_at": None,
            })
    
    # Define schema explicitly to match Iceberg table schema
    schema = StructType([
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
    ])

    df = spark.createDataFrame(harmonized_records, schema=schema)
    return df.withColumn("updated_at", current_timestamp())

def upsert_to_iceberg(spark: SparkSession, harmonized_df) -> None:
    """Upsert data to Iceberg table via MERGE INTO."""
    catalog_name = _env("ICEBERG_CATALOG_NAME", "local")
    database = _env("ICEBERG_DATABASE", "corporate_data")
    table = _env("ICEBERG_TABLE", "corporate_registry")

    full_table = f"{catalog_name}.{database}.{table}"

    # Create table if not exists
    spark.sql(
        f"""
        CREATE DATABASE IF NOT EXISTS {catalog_name}.{database}
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            corporate_id BIGINT,
            corporate_name STRING,
            address STRING,
            activity_places ARRAY<STRING>,
            top_suppliers ARRAY<STRING>,
            supplier_count INT,
            revenue DOUBLE,
            profit DOUBLE,
            main_customers ARRAY<STRING>,
            updated_at TIMESTAMP
        ) USING ICEBERG
        PARTITIONED BY (bucket(16, corporate_id))
        """
    )
    
    # Create temp view for merge
    harmonized_df.createOrReplaceTempView("updates")
    
    # Perform merge operation
    spark.sql(
        f"""
        MERGE INTO {full_table} t
        USING updates s
        ON t.corporate_id = s.corporate_id
        WHEN MATCHED THEN UPDATE SET
          t.corporate_name   = COALESCE(s.corporate_name, t.corporate_name),
          t.address          = COALESCE(s.address, t.address),
          t.activity_places  = COALESCE(s.activity_places, t.activity_places),
          t.top_suppliers    = COALESCE(s.top_suppliers, t.top_suppliers),
          t.supplier_count   = GREATEST(t.supplier_count, s.supplier_count),
          t.revenue          = COALESCE(s.revenue, t.revenue),
          t.profit           = COALESCE(s.profit, t.profit),
          t.main_customers   = COALESCE(s.main_customers, t.main_customers),
          t.updated_at       = COALESCE(s.updated_at, t.updated_at)
        WHEN NOT MATCHED THEN INSERT (
          corporate_id,
          corporate_name,
          address,
          activity_places,
          top_suppliers,
          supplier_count,
          revenue,
          profit,
          main_customers,
          updated_at
        )
        VALUES (
          s.corporate_id,
          s.corporate_name,
          s.address,
          s.activity_places,
          s.top_suppliers,
          s.supplier_count,
          s.revenue,
          s.profit,
          s.main_customers,
          s.updated_at
        )
        """
    )

def main():
    """Main pipeline execution"""
    spark = create_spark_session()
    
    try:
        # Load source data
        supply_df, financial_df = load_source_data(spark)
        
        # Harmonize data
        harmonized_df = harmonize_data(spark, supply_df, financial_df)

        # Data contract check (schema must match Iceberg table)
        validate_corporate_registry_df(harmonized_df)
        
        # Upsert to Iceberg
        upsert_to_iceberg(spark, harmonized_df)
        
        # Read from Iceberg for ML training
        catalog_name = _env("ICEBERG_CATALOG_NAME", "local")
        database = _env("ICEBERG_DATABASE", "corporate_data")
        table = _env("ICEBERG_TABLE", "corporate_registry")
        iceberg_df = spark.sql(f"SELECT * FROM {catalog_name}.{database}.{table}")
        
        # Train ML model
        train_model(iceberg_df)
        
        print("Pipeline completed successfully!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
