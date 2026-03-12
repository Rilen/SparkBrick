import pytest
from pyspark.sql import SparkSession
from src.transformations import transform_silver

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("SparkBrickTests") \
        .getOrCreate()

def test_transform_silver(spark):
    # Setup mock data
    raw_data = [
        (50000.0, 1000000.0, 2.5, "2024-01-01 10:00:00", "API")
    ]
    columns = ["usd", "usd_24h_vol", "usd_24h_change", "ingestion_timestamp", "source"]
    df_raw = spark.createDataFrame(raw_data, columns)
    
    # Run transformation
    df_silver = transform_silver(df_raw)
    
    # Assertions
    assert df_silver.count() == 1
    assert "price_usd" in df_silver.columns
    assert df_silver.collect()[0]["price_usd"] == 50000.0
