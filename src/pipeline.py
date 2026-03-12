from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def process_bronze_to_silver():
    spark = SparkSession.builder.getOrCreate()
    
    # Example paths
    bronze_path = "/mnt/datalake/bronze/raw_data"
    silver_path = "/mnt/datalake/silver/processed_data"
    
    print(f"Reading from {bronze_path}...")
    df = spark.read.format("delta").load(bronze_path)
    
    # Transformation logic
    df_silver = df.withColumn("processed_at", current_timestamp())
    
    print(f"Writing to {silver_path} with Z-Order optimization...")
    df_silver.write.format("delta").mode("overwrite").save(silver_path)
    
    # Apply Z-Order optimization on a specific column (e.g., event_date)
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (event_date)")

if __name__ == "__main__":
    process_bronze_to_silver()
