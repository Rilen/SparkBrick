import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col
from src.transformations import transform_silver, aggregate_gold

# Carrega variáveis do arquivo .env (Host, Token, Serverless)
load_dotenv()

# Este script foi desenhado para rodar via Databricks Interactive Cluster
# Ele consome dados reais da API CoinGecko e percorre as 3 camadas da Medallion Architecture

def run_full_pipeline(spark: SparkSession, symbol="bitcoin", write_to_uc=False):
    print(f"--- Iniciando Pipeline Medallion para {symbol} ---")
    
    # 1. Ingestão (API -> Bronze)
    print("\n[Bronze] Coletando dados da API...")
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true"
    
    response = requests.get(url)
    if response.status_code == 200:
        raw_data = response.json()[symbol]
        df_bronze = spark.createDataFrame([raw_data]) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source", lit("CoinGecko API Live")) \
            .withColumn("asset", lit(symbol))
        
        if write_to_uc:
            print(f"Salvando em sparkbrick.bronze.crypto_raw...")
            df_bronze.write.format("delta").mode("append").saveAsTable("sparkbrick.bronze.crypto_raw")
        else:
            df_bronze.createOrReplaceTempView("bronze_tmp")
            print("Camada Bronze criada em tabela temporária: bronze_tmp")
    else:
        print(f"Erro na API: {response.status_code}")
        return

    # 2. Refino (Bronze -> Silver)
    print("\n[Silver] Limpando e Normalizando...")
    df_silver = transform_silver(df_bronze)
    
    if write_to_uc:
        print(f"Salvando em sparkbrick.silver.crypto_refined e aplicando Z-ORDER...")
        df_silver.write.format("delta").mode("overwrite").saveAsTable("sparkbrick.silver.crypto_refined")
        spark.sql("OPTIMIZE sparkbrick.silver.crypto_refined ZORDER BY (event_time)")
    else:
        df_silver.createOrReplaceTempView("silver_tmp")
        print("Camada Silver processada em silver_tmp")

    # 3. Agregação (Silver -> Gold)
    print("\n[Gold] Calculando KPIs de Negócio...")
    df_gold = aggregate_gold(df_silver)
    
    if write_to_uc:
        print(f"Salvando em sparkbrick.gold.crypto_hourly_kpis...")
        df_gold.write.format("delta").mode("overwrite").saveAsTable("sparkbrick.gold.crypto_hourly_kpis")
    else:
        print("Camada Gold finalizada.")
        df_gold.show()

if __name__ == "__main__":
    # Configuração para Databricks Connect
    try:
        from databricks.connect import DatabricksSession
        builder = DatabricksSession.builder
        
        if os.getenv("DATABRICKS_SERVERLESS") == "true":
            print("[Sessão] Inicializando modo Serverless...")
            builder = builder.serverless()
            
        spark = builder.getOrCreate()
    except ImportError:
        # Fallback para Spark local ou Cluster Interativo tradicional
        print("[Sessão] Databricks Connect não encontrado, usando SparkSession padrão...")
        spark = SparkSession.builder.getOrCreate()
    
    # Executa o pipeline enviando os resultados para o Unity Catalog (sparkbrick.camada.tabela)
    # Certifique-se de que os Schemas (bronze, silver, gold) existam no catálogo sparkbrick
    run_full_pipeline(spark, symbol="ethereum", write_to_uc=True)

