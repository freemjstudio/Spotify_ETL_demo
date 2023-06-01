# Databricks notebook source
# MAGIC %md 
# MAGIC ### 1. Load Dataset
# MAGIC
# MAGIC - user log data 
# MAGIC - contents data

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/mnt/minjiwoo/

# COMMAND ----------

ACCESS_KEY = ""
SECRET_KEY = ""
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cake-demo"
MOUNT_FOLDER = "s3_cake"

MOUNT_DIR = "s3a://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME)
dbutils.fs.mount(MOUNT_DIR, f"/mnt/minjiwoo/{MOUNT_FOLDER}")
display(dbutils.fs.ls(f"/mnt/{MOUNT_FOLDER}"))

df = spark.read.format("csv").load(f"dbfs:/mnt/minjiwoo/{MOUNT_FOLDER}/")


# COMMAND ----------

articles_df = spark.read.csv("dbfs:/mnt/s3_cake/shared_articles.csv", header=True)
articles_df.show(5)
articles_df.write.saveAsTable("shared_articles")

# COMMAND ----------

user_df = spark.read.csv("dbfs:/mnt/s3_cake/users_interactions.csv", header=True)
user_df.show(5)
user_df.write.saveAsTable("user_log")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT DISTINCT(eventType) FROM shared_articles;

# COMMAND ----------

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

# MAGIC %sql 
# MAGIC SELECT DISTINCT(eventType) FROM user_log