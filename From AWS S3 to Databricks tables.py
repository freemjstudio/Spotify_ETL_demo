# Databricks notebook source
# S3 에서 Databricks 환경으로 테이블 옮기기 
# MySQL --[DMS]--> RDS -> S3 bucket 

# df = spark.sql("""
#     CREATE TABLE my_table
#     AS SELECT * 
#     FROM parquet.`s3://freemjstudio-bucket/testdb/member_table/LOAD00000001.parquet`
# """)


# https://medium.com/grabngoinfo/databricks-mount-to-aws-s3-and-import-data-4100621a63fd

# COMMAND ----------

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

aws_keys_df.show()

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe 

ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe='')

# COMMAND ----------

# AWS S3 Bucket Name 
AWS_S3_BUCKET = ''

# Mount Name for the bucket 
MOUNT_NAME = ''

# COMMAND ----------

aws_bucket_name = "freemjstudio-bucket"

df = spark.read.format("parquet").load(f"s3://{aws_bucket_name}/testdb/member_table/LOAD00000001.parquet")
display(df)
dbutils.fs.ls(f"s3://{aws_bucket_name}/")

# SELECT * FROM json.`$path.json`

# COMMAND ----------


