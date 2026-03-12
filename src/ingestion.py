import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

def fetch_crypto_data(symbol="bitcoin"):
    """
    Fetches real-time crypto data (Mocked or real API call).
    In a real scenario, use requests.get(url)
    """
    # Using CoinGecko Free API as example
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data[symbol]
    except Exception as e:
        print(f"Error fetching data: {e}")
        # Fallback mock data if API is down
        return {"usd": 65000.0, "usd_24h_vol": 25000000.0, "usd_24h_change": 1.5}

def ingest_to_bronze(spark: SparkSession, data: dict, output_path: str):
    """
    Saves raw data to Bronze layer with Schema Evolution.
    """
    df = spark.createDataFrame([data]) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source", lit("CoinGecko API"))
    
    # Writing with mergeSchema for evolution
    df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("append") \
        .save(output_path)
    
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    data = fetch_crypto_data()
    ingest_to_bronze(spark, data, "/mnt/sparkbrick/bronze/crypto_raw")
