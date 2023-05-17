# Databricks notebook source
# S3 에서 Databricks 환경으로 테이블 옮기기 
# MySQL --[DMS]--> RDS -> S3 bucket 

# df.write.option("path", "s3://my-bucket/external-location/path/to/table").saveAsTable("my_table")
# s3://freemjstudio-bucket/export-s3/export_info_export-s3.json
df.write.option("path","s3://freemjstudio-bucket/export-s3/export_info_export-s3.json").saveAsTable("my_table")

spark.sql("""
    CREATE TABLE my_table
    AS SELECT * 
    FROM parquet.`s3://my-bucket/external-location/path/to/data`
""")


# COMMAND ----------


