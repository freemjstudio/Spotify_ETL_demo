# Databricks notebook source
# MAGIC %md 
# MAGIC ## External Location 에서 S3 버킷 바로 접근하기 

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

display(spark.sql("DESCRIBE EXTERNAL LOCATION `cake-test`"))

# COMMAND ----------

spark.sql("""
  CREATE TABLE cake_user
  AS (SELECT *
    FROM parquet.`s3://cake-parquet/user/`)
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM cake_user LIMIT 10;

# COMMAND ----------

spark.read.format("parquet").load("s3://cake-parquet/user/").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW STORAGE CREDENTIALS

# COMMAND ----------

# view 만들고 # table 만들기 
spark.sql("""
          CREATE VIEW cake_mongo_view 
          AS (SELECT * FROM csv.`s3://minji-spotify-mongodb/cake/user/`)
          """)

# COMMAND ----------

spark.sql("""
  CREATE TABLE cake_mongo
  AS (SELECT *
    FROM cake_mongo_view)
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE VIEW cake_mongo_view (Op STRING, dmsTimestamp timestamp, _id STRING, _doc STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ","
# MAGIC )
# MAGIC LOCATION "s3://minji-spotify-mongodb/cake/user"

# COMMAND ----------

spark.read.format("parquet").load("s3://cake-parquet").show()

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %sql 
# MAGIC LIST `s3://minji-spotify-mongodb/cake/user`

# COMMAND ----------


