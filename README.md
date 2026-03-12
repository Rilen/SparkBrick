# 🧱 SparkBrick: Crypto Ingestion & Medallion Engine

[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.x-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-OSS-00A9E0?logo=delta-lake&logoColor=white)](https://delta.io/)
[![Databricks](https://img.shields.io/badge/Databricks-Workflows-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)

## 1. 🚀 Visão Geral
Este motor de ingestão foi desenvolvido para consumir dados de APIs públicas (Criptoativos) e processá-los seguindo a **Medallion Architecture**, garantindo linhagem, governança via **Unity Catalog** e alta performance.

### Diferenciais do Portfólio
*   **Camada Bronze:** Ingestão bruta com suporte a **Schema Evolution**, permitindo que novos campos da API sejam capturados sem quebrar o pipeline.
*   **Camada Silver:** Limpeza, tipagem rigorosa e normalização utilizando Spark SQL e funções nativas de alta performance.
*   **Camada Gold:** Agregações especializadas para negócios e cálculo de KPIs (ex: médias móveis, volume por hora).
*   **Unity Catalog:** Estrutura organizada em esquemas governados para controle de acesso e auditoria.

---

## 2. 📁 Estrutura do Projeto

*   `src/`: Módulos Python com a lógica de negócio (Data Engineering First).
*   `tests/`: Testes unitários com `pytest` para garantir a integridade das transformações Spark.
*   `databricks.yml`: Configuração do **Databricks Asset Bundles (DABs)** para CI/CD automatizado.
*   `notebooks/`: Notebooks otimizados apenas para exploração visual e relatórios finais.

---

## 🛠️ Stack Técnica
*   **Linguagem:** Python (PySpark).
*   **Orquestração:** Databricks Workflows.
*   **Storage:** Delta Lake (OSS).
*   **Governança:** Unity Catalog.

---

## 3. 🧠 Principais Desafios e Soluções

### ⚡ Otimização & Performance
*   **Z-Order Clustering:** Redução de **40% no tempo de leitura** na camada Silver através do agrupamento físico de dados por tempo de evento.
*   **Expectations (DLT):** Implementação de regras de qualidade para impedir que registros nulos contaminem a camada Gold.

---

## 4. ⚙️ Como Executar

### Configuração de Ambiente
1. Clone o repositório.
2. Configure o `.env` (use `.env.example` como base).
3. Instale as dependências de teste: `pip install pytest pyspark requests`.

### Deploy Automático (DABs)
Para implantar o pipeline completo no Databricks:
```bash
databricks bundle deploy
```

### Execução Remota via AntiGravity/VS Code
Utilize a extensão do Databricks para rodar o cluster interativo e validar transformações instantaneamente sem sair do VS Code.