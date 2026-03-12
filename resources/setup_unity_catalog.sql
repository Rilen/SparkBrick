-- 🛡️ Configuração do Unity Catalog para o Projeto SparkBrick
-- Execute estes comandos no seu Databricks SQL Warehouse ou em um Notebook

-- 1. Criar o Catálogo (requer privilégios de ACCOUNT ADMIN ou metatada privileges)
CREATE CATALOG IF NOT EXISTS sparkbrick;

-- 2. Definir o uso do catálogo
USE CATALOG sparkbrick;

-- 3. Criar os Schemas (Camadas da Medallion)
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Dados brutos ingeridos diretamente das APIs';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Dados limpos, normalizados e otimizados com Z-Order';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Dados agregados e KPIs de negócio para consumo analítico';

-- 4. (Opcional) Conceder permissões para o grupo de analistas
-- GRANT USAGE ON CATALOG sparkbrick TO `analysts_group`;
-- GRANT SELECT ON SCHEMA gold TO `analysts_group`;

-- 5. Listar schemas para validar
SHOW SCHEMAS;
