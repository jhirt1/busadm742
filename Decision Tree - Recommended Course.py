# Databricks notebook source
# MAGIC %md
# MAGIC After exploring the data set further and experimenting with traditional collaborative filtering techniques, we will need to move forward with a decision tree. This is based on the fact that the dataset that I'm using has only a few students that have taken more than one class on the platform. Thus the features of the students will help dictate course performance.

# COMMAND ----------

from sklearn.preprocessing import LabelEncoder
import pandas as pd
from sklearn.model_selection import train_test_split
import warnings
warnings.simplefilter(action='ignore')

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
# MAGIC WHERE imd_band is not null
# MAGIC AND final_result NOT IN ('Fail', 'Withdrawn');

# COMMAND ----------

df = _sqldf.toPandas()
df.head(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning

# COMMAND ----------

df.drop(columns=['code_presentation'], inplace=True)
df.head(3)

# COMMAND ----------

X = df[['gender', 'region', 'highest_education',
       'imd_band', 'age_band', 'studied_credits', 'disability']]

# COMMAND ----------

X['gender'] = X['gender'].astype('category')
X['region'] = X['region'].astype('category')
X['highest_education'] = X['highest_education'].astype('category')
X['imd_band'] = X['imd_band'].astype('category')
X['age_band'] = X['age_band'].astype('category')
X['disability'] = X['disability'].astype('category')

# COMMAND ----------

gen_map = dict( zip( X['gender'].cat.codes, X['gender'] ) )
region_map = dict( zip( X['region'].cat.codes, X['region'] ) )
ed_map = dict( zip( X['highest_education'].cat.codes, X['highest_education'] ) )
imd_map = dict( zip( X['imd_band'].cat.codes, X['imd_band'] ) )
age_map = dict( zip( X['age_band'].cat.codes, X['age_band'] ) )
dis_map = dict( zip( X['disability'].cat.codes, X['disability'] ) )

# COMMAND ----------

X['gender'] = X['gender'].map(dict((v,k) for k,v in gen_map.items()))
X['region'] = X['region'].map(dict((v,k) for k,v in region_map.items()))
X['highest_education'] = X['highest_education'].map(dict((v,k) for k,v in ed_map.items()))
X['imd_band'] = X['imd_band'].map(dict((v,k) for k,v in imd_map.items()))
X['age_band'] = X['age_band'].map(dict((v,k) for k,v in age_map.items()))
X['disability'] = X['disability'].map(dict((v,k) for k,v in dis_map.items()))
X

# COMMAND ----------

features = X.columns.to_list()

# COMMAND ----------

Y = df[['code_module', 'final_result']]

# COMMAND ----------

Y['final_result'].unique

# COMMAND ----------

classes = {'AAA': 0, 'BBB': 1, 'CCC': 2, 'DDD': 3, 'EEE': 4, 'FFF': 5, 'GGG': 6}
final_result = {'Fail': 0, 'Withdrawn': 1, 'Pass': 2, 'Distinction': 3}
Y['coded_module'] = Y['code_module'].map(classes)
Y['result'] = Y['final_result'].map(final_result)
Y.head(3)

# COMMAND ----------

full_set = pd.concat([X, Y], axis=1)
full_set

# COMMAND ----------

train, test = train_test_split(full_set, test_size=0.25, random_state=87)

# COMMAND ----------

from sklearn import tree
from sklearn.tree import DecisionTreeClassifier
import matplotlib.pyplot as plt

# COMMAND ----------

full_set.columns

# COMMAND ----------

features = ['gender', 'region', 'highest_education', 'imd_band', 'age_band',
       'studied_credits', 'disability']
targets = ['code_module', 'final_result',
       'coded_module', 'result']

# COMMAND ----------

dtree = DecisionTreeClassifier(max_depth=7)
dtree = dtree.fit(train[features], train['coded_module'])

# COMMAND ----------

# tree.plot_tree(dtree, feature_names=features)

# COMMAND ----------

score = dtree.score(test[features], test['coded_module'])
score

# COMMAND ----------

test[features].iloc[0].to_numpy()

# COMMAND ----------

print(dtree.predict([test[features].iloc[3].to_numpy()]))

# COMMAND ----------

test.iloc[3]

# COMMAND ----------

fig = plt.figure(figsize=(25,15))
_ = tree.plot_tree(dtree, 
                   fontsize=8,
                   feature_names=features,
                   class_names = ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'GGG'],
                   max_depth= 7,
                   filled=True)
