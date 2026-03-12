# Databricks Notebook source
# MAGIC %md
# MAGIC # 📈 Crypto Explorer
# MAGIC Utilize este notebook para visualizar as agregações da camada Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Visualização dos KPIs na camada Gold
# MAGIC SELECT * FROM sparkbrick_gold.crypto_hourly_stats
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# Exemplo de visualização com PySpark
display(spark.table("sparkbrick_gold.crypto_hourly_stats"))
