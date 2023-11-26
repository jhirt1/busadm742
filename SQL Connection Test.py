# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("busadm742")

# COMMAND ----------

dbutils.secrets.get(scope="busadm742", key="sqlPWD")

# COMMAND ----------

remote_table = (spark.read
  .format("sqlserver")
  .option("host", "busadm742.database.windows.net")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "busadm742")
  .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
  .option("database", "open-learning-analytics")
  .option("dbtable", "dbo.courses") # (if schemaName not provided, default to "dbo")
  .load()
)

# COMMAND ----------

remote_table.display()
