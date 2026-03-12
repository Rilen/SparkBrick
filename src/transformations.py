from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round, avg, window

def transform_silver(df: DataFrame) -> DataFrame:
    """
    Cleans and normalizes crypto data.
    """
    return df.select(
        col("usd").alias("price_usd"),
        col("usd_24h_vol").alias("volume_24h"),
        col("usd_24h_change").alias("change_24h_pct"),
        col("ingestion_timestamp").cast("timestamp").alias("event_time"),
        col("source")
    ).filter(col("price_usd") > 0)

def aggregate_gold(df: DataFrame) -> DataFrame:
    """
    Computes KPIs and business aggregations.
    """
    return df.groupBy(
        window(col("event_time"), "1 hour")
    ).agg(
        round(avg("price_usd"), 2).alias("avg_price_hourly"),
        round(avg("volume_24h"), 2).alias("avg_volume_hourly")
    ).select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        "avg_price_hourly",
        "avg_volume_hourly"
    )
