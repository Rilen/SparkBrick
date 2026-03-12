import os
import requests
from dotenv import load_dotenv
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Carregar credenciais
load_dotenv()

def ingest_ibge_data():
    print("--- Iniciando Ingestão Comparativa IBGE (Região dos Lagos/Norte Fluminense) ---")
    
    # Mapeamento de cidades e códigos IBGE
    cities = {
        "3304524": "Rio das Ostras",
        "3302403": "Macaé",
        "3301306": "Casimiro de Abreu",
        "3300704": "Cabo Frio",
        "3300258": "Búzios"
    }
    
    city_ids = "|".join(cities.keys())
    indicators = "29168|29171|29169|29172" # Pop, PIB, Área, IDH
    url = f"https://servicodados.ibge.gov.br/api/v1/pesquisas/indicadores/{indicators}/resultados/{city_ids}"
    
    print(f"Buscando dados comparativos na API do IBGE...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        rows = []
        
        names = {
            29168: "Populacao",
            29171: "PIB_per_capita",
            29169: "Area_Territorial",
            29172: "IDH_Municipal"
        }
        
        for item in data:
            indicator_id = item['id']
            indicator_name = names.get(indicator_id, f"Indicador_{indicator_id}")
            
            for loc in item['res']:
                city_id = loc['localidade']
                city_name = cities.get(city_id, "Desconhecido")
                
                # Pega o último resultado disponível
                res_dict = loc['res']
                latest_year = sorted(res_dict.keys())[-1]
                value = res_dict[latest_year]
                
                # Tratamento robusto do valor
                clean_value = 0.0
                if value and isinstance(value, str) and value != '-':
                    try:
                        clean_value = float(value.replace(',', '.'))
                    except ValueError: clean_value = 0.0
                elif isinstance(value, (int, float)):
                    clean_value = float(value)
                
                rows.append({
                    "municipio": city_name,
                    "codigo_ibge": city_id,
                    "indicador": indicator_name,
                    "valor": clean_value,
                    "ano_referencia": latest_year
                })
            
        # Conectar ao Databricks
        print("Enviando dados para o Databricks...")
        builder = DatabricksSession.builder
        if os.getenv("DATABRICKS_SERVERLESS") == "true":
            builder = builder.serverless()
        spark = builder.getOrCreate()
        
        df = spark.createDataFrame(rows).withColumn("ingestion_time", current_timestamp())
        
        # Salvar/Sobrescrever
        table_name = "sparkbrick.bronze.ibge_comparativo_regional"
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"✅ Tabela {table_name} atualizada com {len(rows)} registros.")

        
    else:
        print(f"Erro ao acessar API do IBGE: {response.status_code}")

if __name__ == "__main__":
    ingest_ibge_data()
