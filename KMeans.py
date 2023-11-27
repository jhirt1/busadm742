# Databricks notebook source
studentInfo_table = (spark.read
  .format("sqlserver")
  .option("host", "busadm742.database.windows.net")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "busadm742")
  .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
  .option("database", "open-learning-analytics")
  .option("dbtable", "dbo.studentInfo") # (if schemaName not provided, default to "dbo")
  .load()
)
studentInfo_table.createOrReplaceTempView('studentInfo')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM studentInfo
# MAGIC LIMIT 1;

# COMMAND ----------

import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
import seaborn as sns
import numpy as np
from sklearn.cluster import KMeans

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_student,
# MAGIC code_module,
# MAGIC highest_education,
# MAGIC age_band,
# MAGIC region,
# MAGIC disability,
# MAGIC final_result
# MAGIC from studentInfo;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base as (
# MAGIC   SELECT 
# MAGIC     id_student, 
# MAGIC     COUNT(id_student) as count
# MAGIC   FROM studentInfo
# MAGIC   GROUP BY id_student
# MAGIC )
# MAGIC SELECT 
# MAGIC   count(id_student)
# MAGIC FROM base
# MAGIC WHERE count > 3;

# COMMAND ----------

df = _sqldf.toPandas()
df.head(3)

# COMMAND ----------

oh_df = pd.get_dummies(df, columns = ['code_module', 'highest_education', 'age_band', 'region', 'disability', 'final_result'])
oh_df.head(3)

# COMMAND ----------

oh_df.columns

# COMMAND ----------

train_set, test_set = train_test_split(oh_df[['code_module_AAA', 'code_module_BBB', 'code_module_CCC',
       'code_module_DDD', 'code_module_EEE', 'code_module_FFF',
       'code_module_GGG', 'highest_education_A Level or Equivalent',
       'highest_education_HE Qualification',
       'highest_education_Lower Than A Level',
       'highest_education_No Formal quals',
       'highest_education_Post Graduate Qualification', 'age_band_0-35',
       'age_band_35-55', 'age_band_55<=', 'region_East Anglian Region',
       'region_East Midlands Region', 'region_Ireland', 'region_London Region',
       'region_North Region', 'region_North Western Region', 'region_Scotland',
       'region_South East Region', 'region_South Region',
       'region_South West Region', 'region_Wales',
       'region_West Midlands Region', 'region_Yorkshire Region',
       'disability_False', 'disability_True', 'final_result_Distinction',
       'final_result_Fail', 'final_result_Pass', 'final_result_Withdrawn']], test_size = 0.25, random_state=87)

# COMMAND ----------

km = KMeans(n_clusters = 5)
km.fit(train_set)

# COMMAND ----------

train_set['labels'] = km.labels_

# COMMAND ----------

train_set

# COMMAND ----------

preds = km.predict(test_set.to_numpy())

# COMMAND ----------

test_set['labels'] = preds

# COMMAND ----------

test_set.head(3)

# COMMAND ----------

train_set[(train_set['labels']==0) & (train_set['code_module_AAA'] == 1)][['final_result_Distinction', 'final_result_Fail', 'final_result_Pass', 'final_result_Withdrawn']].sum()

# COMMAND ----------

test_set[(test_set['labels']==0) & (test_set['code_module_AAA'] == 1)][['final_result_Distinction', 'final_result_Fail', 'final_result_Pass', 'final_result_Withdrawn']].sum()

# COMMAND ----------

train_set[(train_set['labels']==0) & (train_set['code_module_BBB'] == 1)][['final_result_Distinction', 'final_result_Fail', 'final_result_Pass', 'final_result_Withdrawn']].sum()

# COMMAND ----------

test_set[(test_set['labels']==0) & (test_set['code_module_BBB'] == 1)][['final_result_Distinction', 'final_result_Fail', 'final_result_Pass', 'final_result_Withdrawn']].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing recommendation
# MAGIC
# MAGIC For person X in cluster Y, how will they perform based on a simlar person in the cluster?

# COMMAND ----------

test_set.iloc[1]
#cluster 0 - test module BBB
#answer Pass

# COMMAND ----------

reps = train_set[(train_set['code_module_BBB']==1) & (train_set['labels'] == 0)]
reps
