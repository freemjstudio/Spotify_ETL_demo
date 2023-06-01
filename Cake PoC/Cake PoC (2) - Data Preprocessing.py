# Databricks notebook source
import numpy as np
import scipy
import pandas as pd
import math
import random
import sklearn
from nltk.corpus import stopwords
from scipy.sparse import csr_matrix
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse.linalg import svds
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

# COMMAND ----------

user_df = spark.read.table("user_log")
shared_df = spark.read.table("shared_articles")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM user_log;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(DISTINCT(personId)) FROM user_log; 
# MAGIC -- user 는 총 1895명 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(timestamp) FROM user_log WHERE timestamp IS NOT NULL; 
# MAGIC -- 결측값 없음 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM user_log WHERE personId = 344280948527967603 ORDER BY timestamp ASC

# COMMAND ----------

# 동일한 타임스탬프 & personId 가 있는 경우 하나는 지우기 
user_df = user_df.drop_duplicates(['timestamp', 'personId'])

# COMMAND ----------

# 기존의 로그가 없는 경우 timestamp 계산 x 
# (현재 접속 시간 - 바로 이전 접속 시간) 

t1 = 1486646147
t2 = 1486646177
m = float((t2-t1) / 60)
d = int(m/24)
print(d)

# COMMAND ----------

# timestamp 로 일 수 계산하기 ? 
# //timestamp의 단위는 초(seconds)이기 때문에 60으로 나눠주면 분이 된다.
# $d_day_m = floor(($date_timestamp - $current_time) / 60); 

# //분 데이터를 24로 나눠서 day 수를 구함
# //24시간 미만일때는 0일로 처리
# $d_day_d = floor($d_day_m / 24);


# COMMAND ----------



# COMMAND ----------

from pyspark.sql import functions as F

user_df = user_df.orderBy(F.col("timestamp").asc()) 

# COMMAND ----------

user_df['blank_days'] = 

# time stamp 수로 학습하지 않은 일 수 계산 

# COMMAND ----------

event_type_strength = {
   'VIEW': 1.0,
   'LIKE': 2.0, 
   'BOOKMARK': 2.5, 
   'FOLLOW': 3.0,
   'COMMENT CREATED': 4.0,  
}
user_df['eventStrength'] = 

# COMMAND ----------

articles_df = articles_df[articles_df['eventType'] == 'CONTENT SHARED']

# COMMAND ----------

# MAGIC %md 
# MAGIC 전처리결과 테이블에 write