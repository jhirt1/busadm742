# Databricks notebook source
# MAGIC %md
# MAGIC # EDA and Training

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gather Data From SQL
# MAGIC
# MAGIC __Tables__
# MAGIC * assessments
# MAGIC * courses
# MAGIC * studentAssessment
# MAGIC * studentInfo
# MAGIC * studentRegistration
# MAGIC * studentVle
# MAGIC * vle

# COMMAND ----------

# assessments_table = (spark.read
#   .format("sqlserver")
#   .option("host", "busadm742.database.windows.net")
#   .option("port", "1433") # optional, can use default port 1433 if omitted
#   .option("user", "busadm742")
#   .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
#   .option("database", "open-learning-analytics")
#   .option("dbtable", "dbo.assessments") # (if schemaName not provided, default to "dbo")
#   .load()
# )
# assessments_table.createOrReplaceTempView('assessments')

# COMMAND ----------

# %sql
# SELECT *
# FROM assessments
# LIMIT 1;

# COMMAND ----------

courses_table = (spark.read
  .format("sqlserver")
  .option("host", "busadm742.database.windows.net")
  .option("port", "1433") # optional, can use default port 1433 if omitted
  .option("user", "busadm742")
  .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
  .option("database", "open-learning-analytics")
  .option("dbtable", "dbo.courses") # (if schemaName not provided, default to "dbo")
  .load()
)
courses_table.createOrReplaceTempView('courses')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM courses
# MAGIC LIMIT 1;

# COMMAND ----------

# studentAssessment_table = (spark.read
#   .format("sqlserver")
#   .option("host", "busadm742.database.windows.net")
#   .option("port", "1433") # optional, can use default port 1433 if omitted
#   .option("user", "busadm742")
#   .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
#   .option("database", "open-learning-analytics")
#   .option("dbtable", "dbo.studentAssessment") # (if schemaName not provided, default to "dbo")
#   .load()
# )
# studentAssessment_table.createOrReplaceTempView('studentAssessment')

# COMMAND ----------

# %sql 
# SELECT *
# FROM studentAssessment
# LIMIT 1;

# COMMAND ----------

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

# studentRegistration_table = (spark.read
#   .format("sqlserver")
#   .option("host", "busadm742.database.windows.net")
#   .option("port", "1433") # optional, can use default port 1433 if omitted
#   .option("user", "busadm742")
#   .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
#   .option("database", "open-learning-analytics")
#   .option("dbtable", "dbo.studentRegistration") # (if schemaName not provided, default to "dbo")
#   .load()
# )
# studentRegistration_table.createOrReplaceTempView('studentRegistration')

# COMMAND ----------

# %sql
# SELECT *
# FROM studentRegistration
# LIMIT 1;

# COMMAND ----------

# studentVle_table = (spark.read
#   .format("sqlserver")
#   .option("host", "busadm742.database.windows.net")
#   .option("port", "1433") # optional, can use default port 1433 if omitted
#   .option("user", "busadm742")
#   .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
#   .option("database", "open-learning-analytics")
#   .option("dbtable", "dbo.studentVle") # (if schemaName not provided, default to "dbo")
#   .load()
# )
# studentVle_table.createOrReplaceTempView('studentVle')

# COMMAND ----------

# %sql
# SELECT *
# FROM studentVle
# LIMIT 1;

# COMMAND ----------

# vle_table = (spark.read
#   .format("sqlserver")
#   .option("host", "busadm742.database.windows.net")
#   .option("port", "1433") # optional, can use default port 1433 if omitted
#   .option("user", "busadm742")
#   .option("password", dbutils.secrets.get(scope="busadm742", key="sqlPWD"))
#   .option("database", "open-learning-analytics")
#   .option("dbtable", "dbo.vle") # (if schemaName not provided, default to "dbo")
#   .load()
# )
# vle_table.createOrReplaceTempView('vle')

# COMMAND ----------

# %sql
# SELECT *
# FROM vle
# LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## EDA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT id_student)
# MAGIC FROM studentInfo;
# MAGIC
# MAGIC -- There are 28,785 unique students in the data set

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT code_module)
# MAGIC FROM courses;
# MAGIC
# MAGIC -- There are 7 distinct courses

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (SELECT id_student,
# MAGIC   COUNT(c.code_module) AS courses
# MAGIC FROM studentInfo as si 
# MAGIC   INNER JOIN courses as c
# MAGIC     ON si.code_module = c.code_module
# MAGIC GROUP BY id_student)
# MAGIC SELECT
# MAGIC   MIN(courses),
# MAGIC   MAX(courses),
# MAGIC   AVG(courses)
# MAGIC FROM base;
# MAGIC
# MAGIC -- The minimum number of courses taken is 2
# MAGIC -- The maximum number of courses taken is 16
# MAGIC -- The average number of courses taken is 3.97916

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM studentInfo;

# COMMAND ----------

# MAGIC %md 
# MAGIC First pass at features
# MAGIC * id_student
# MAGIC * code_module
# MAGIC * final_result
# MAGIC * gender --> removed since it is non-inclusive
# MAGIC * region
# MAGIC * highest_education
# MAGIC * disability --> too skewed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT code_module, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY code_module;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT final_result, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY final_result;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT gender, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY gender;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT highest_education, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY highest_education;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT disability, COUNT(*) as student_attempts
# MAGIC FROM studentInfo
# MAGIC GROUP BY disability;

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collaborative Filtering - Training

# COMMAND ----------

! pip install numpy

# COMMAND ----------

! pip install scikit-surprise

# COMMAND ----------

import pandas as pd
from surprise import Reader, KNNWithMeans, Dataset, accuracy, SVD
from surprise.model_selection import train_test_split
from surprise.model_selection import cross_validate
from sklearn.metrics.pairwise import cosine_similarity
import seaborn as sns
import numpy as np

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_student as user,
# MAGIC   code_module as item,
# MAGIC   final_result as rating
# MAGIC FROM studentInfo;

# COMMAND ----------

df = _sqldf.toPandas()
df.head(3)

# COMMAND ----------

df['rating'].unique()
# Fail = 1, Withdrawn = 2, Pass = 3, Distinction = 4

# COMMAND ----------

def final_result_encoder(x):
    if x == 'Fail':
        return 1
    elif x == 'Withdrawn':
        return 2
    elif x == 'Pass':
        return 3
    elif x == 'Distinction':
        return 4

# COMMAND ----------

df['rating'] = df['rating'].apply(final_result_encoder)
df.head(3)

# COMMAND ----------

reader = Reader(rating_scale=(1,4))
data = Dataset.load_from_df(df, reader)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Testing SVD

# COMMAND ----------

algo = SVD()
cross_validate(algo, data, measures=["RMSE", "MAE"], cv=5, verbose=True)

# COMMAND ----------

trainset, testset = train_test_split(data, test_size=0.25)

# COMMAND ----------

algo = SVD()

# COMMAND ----------

algo.fit(trainset)
predictions = algo.test(testset)

# COMMAND ----------

accuracy.rmse(predictions)

# COMMAND ----------

accuracy.mae(predictions)

# COMMAND ----------

predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ### KNN

# COMMAND ----------

trainingSet = data.build_full_trainset()

# COMMAND ----------

from surprise import KNNWithMeans


sim_options = {
    "name": "cosine",
    "user_based": False, 
}
algo = KNNWithMeans(sim_options=sim_options)

# COMMAND ----------

algo.fit(trainingSet)

# COMMAND ----------



# COMMAND ----------

prediction = algo.predict('E', 2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Traditional clustering

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

df = _sqldf.toPandas()
df.head(3)

# COMMAND ----------

oh_df = pd.get_dummies(df, columns = ['code_module', 'highest_education', 'age_band', 'region', 'disability', 'final_result'])
oh_df.head(3)

# COMMAND ----------

from sklearn.cluster import KMeans
km = KMeans(n_clusters = 5)
km.fit(oh_df)
km.predict(oh_df)
oh_df['labels'] = km.labels_

# COMMAND ----------

oh_df[oh_df['labels']==2]
