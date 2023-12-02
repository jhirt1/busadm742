# Databricks notebook source
filesystem = 'models'
storage_name = 'busadm742'

# COMMAND ----------

filesystem_url = f"abfss://{filesystem}@{storage_name}.dfs.core.windows.net"
