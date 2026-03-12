from databricks.connect import DatabricksSession
import os

def test_connection():
    print("Tentando conectar ao Databricks via Databricks Connect...")
    
    try:
        # Tenta criar a sessão usando as credenciais configuradas no ambiente
        builder = DatabricksSession.builder
        
        # Se a variável SERVERLESS estiver configurada, forçamos o uso dela no builder
        if os.environ.get("DATABRICKS_SERVERLESS") == "true":
            print("Configurando sessão para modo SERVERLESS...")
            builder = builder.serverless()
            
        spark = builder.getOrCreate()
        
        print("\n✅ Conexão estabelecida com sucesso!")
        
        # Teste simples: Listar catálogos no Unity Catalog
        print("\nListando catálogos disponíveis:")
        spark.sql("SHOW CATALOGS").show()
        
        # Teste de processamento Spark local -> remoto
        print("Criando um DataFrame de teste...")
        data = [("SparkBrick", 1), ("Databricks", 2), ("Connect", 3)]
        df = spark.createDataFrame(data, ["componente", "id"])
        df.show()
        
        print("\n🚀 Tudo pronto para o desenvolvimento local!")
        
    except Exception as e:
        print("\n❌ Erro ao conectar:")
        print(str(e))
        print("\nCertifique-se de que:")
        print("1. O terminal tem as variáveis DATABRICKS_HOST e DATABRICKS_TOKEN configuradas.")
        print("2. O cluster de desenvolvimento está ligado.")

if __name__ == "__main__":
    test_connection()
