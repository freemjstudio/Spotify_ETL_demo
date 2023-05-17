# Databricks notebook source
# S3 에서 Databricks 환경으로 테이블 옮기기 
# MySQL --[DMS]--> RDS -> S3 bucket 

# df.write.option("path", "s3://my-bucket/external-location/path/to/table").saveAsTable("my_table")
# s3://freemjstudio-bucket/export-s3/export_info_export-s3.json


# df.write.option("path","s3://freemjstudio-bucket/export-s3/export_info_export-s3.json").saveAsTable("my_table")

# spark.sql("""
#     CREATE TABLE my_table
#     AS SELECT * 
#     FROM json.`s3://freemjstudio-bucket/export-s3/export_info_export-s3.json`
# """)


# https://medium.com/grabngoinfo/databricks-mount-to-aws-s3-and-import-data-4100621a63fd

# COMMAND ----------

aws_bucket_name = "freemjstudio-bucket"

df = spark.read.format("json").load(f"s3://{aws_bucket_name}/export-s3/export_info_export-s3.json")
display(df)
dbutils.fs.ls(f"s3://{aws_bucket_name}/")

# SELECT * FROM json.`$path.json`

# COMMAND ----------

# Mount S3 Bucket 

from pyspark.sql.functions import *
import urllib

# file type 
file_type = "csv"

first_row_is_header = "true"
delimiter = ","

# Read CSV file to spark dataframe 

aws_keys_df = spark.read.format(file_type).option("header", first_row_is_header).option("sep", delimiter).load("")


# COMMAND ----------


