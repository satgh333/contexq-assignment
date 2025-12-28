from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName(
        "ReadIcebergCorporateRegistry"
    ).getOrCreate()

    df = spark.read.table(
        "glue_catalog.corporate_db.corporate_registry"
    )

    print("===== Iceberg Table: corporate_registry =====")
    df.show(truncate=False)
    df.printSchema()
    print(f"Total rows: {df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()
