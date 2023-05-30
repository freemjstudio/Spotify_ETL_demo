# Databricks notebook source
# MAGIC %sql
# MAGIC desc detail test_access_keys

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/test_access_keys")

# COMMAND ----------

from pyspark.sql.functions import *  # pyspark functions 

# URL processing 
import urllib 

file_type = "delta" 

# Whether the file has a header
first_row_is_header = "true"

# Delimiter used in the file 
delimiter = ","

# Read CSV file to spark dataframe 
aws_keys_df = spark.read.format(file_type).option("header", first_row_is_header).option("sep", delimiter).load("dbfs:/user/hive/warehouse/test_access_keys")


# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe 
ACCESS_KEY = 'YOUR_ACCESS_KEY'
SECRET_KEY = 'YOUR_SECRET_KEY'
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')

# COMMAND ----------

# AWS S3 Bucket Name 
AWS_S3_BUCKET = 'freemjstudio-bucket'

# Mount Name for the bucket 
MOUNT_NAME = '/mnt/freemjstudio-bucket'

SOURCE_URL = 's3n://{0}:{1}@{2}'.format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)


# COMMAND ----------

dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/freemjstudio-bucket/testdb"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 외부 저장소 (aws S3 Bucket)에서 data 읽어오기

# COMMAND ----------

aws_bucket_name = "freemjstudio-bucket"
df = spark.read.format("parquet").load(f"s3://{aws_bucket_name}/testdb/member_table/LOAD00000001.parquet")
display(df)

# COMMAND ----------

df = spark.sql("""
    CREATE TABLE my_table
    AS SELECT * 
    FROM parquet.`s3://freemjstudio-bucket/testdb/member_table/LOAD00000001.parquet`
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM my_table;

# COMMAND ----------

df = spark.sql("""
    CREATE TABLE employee
    AS SELECT * 
    FROM parquet.`s3://freemjstudio-bucket/testdb/member_table/LOAD00000001.parquet`
""")
