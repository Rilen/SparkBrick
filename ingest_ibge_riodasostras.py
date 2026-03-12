import os
import requests
from dotenv import load_dotenv
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Carregar credenciais
load_dotenv()

def ingest_ibge_data():
    print("--- Iniciando Ingestão de Dados IBGE (Rio das Ostras) ---")
    
    # Código IBGE para Rio das Ostras: 3304524 (Município)
    # Vamos pegar alguns indicadores: 29168 (População), 29171 (PIB), 29169 (Área)
    # Adicionando mais alguns interessantes para análise de cientista
    # 29170 (Densidade demográfica), 29172 (IDHM)
    indicators = "29168|29171|29169|29170|29172"
    url = f"https://servicodados.ibge.gov.br/api/v1/pesquisas/indicadores/{indicators}/resultados/3304524"
    
    print(f"Buscando dados na API do IBGE...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        rows = []
        
        # Mapeamento manual para facilitar a leitura do cientista
        names = {
            29168: "Populacao",
            29171: "PIB_per_capita",
            29169: "Area_Territorial",
            29170: "Densidade_Demografica",
            29172: "IDH_Municipal"
        }
        
        for item in data:
            indicator_id = item['id']
            indicator_name = names.get(indicator_id, f"Indicador_{indicator_id}")
            
            # Pega o último resultado disponível (geralmente o ano mais recente)
            res_list = item['res'][0]['res']
            latest_year = sorted(res_list.keys())[-1]
            value = res_list[latest_year]
            
            # Tratamento robusto do valor
            clean_value = 0.0
            if value and isinstance(value, str) and value != '-':
                try:
                    clean_value = float(value.replace(',', '.'))
                except ValueError:
                    clean_value = 0.0
            elif isinstance(value, (int, float)):
                clean_value = float(value)
            
            rows.append({
                "municipio": "Rio das Ostras",
                "codigo_ibge": "3304524",
                "indicador": indicator_name,
                "valor": clean_value,
                "ano_referencia": latest_year
            })
            
        # Conectar ao Databricks
        print("Conectando ao Databricks...")
        builder = DatabricksSession.builder
        if os.getenv("DATABRICKS_SERVERLESS") == "true":
            builder = builder.serverless()
        spark = builder.getOrCreate()
        
        # Criar DataFrame
        df = spark.createDataFrame(rows) \
            .withColumn("ingestion_time", current_timestamp())
        
        print("\nDados processados localmente:")
        df.show()
        
        # Salvar no Unity Catalog
        table_name = "sparkbrick.bronze.ibge_rio_das_ostras"
        print(f"Salvando em {table_name}...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        
        print("\n✅ Sucesso! Agora você pode ver os dados da sua cidade no Databricks.")
        
    else:
        print(f"Erro ao acessar API do IBGE: {response.status_code}")

if __name__ == "__main__":
    ingest_ibge_data()
