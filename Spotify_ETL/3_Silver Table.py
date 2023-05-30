# Databricks notebook source
# MAGIC %md 
# MAGIC ## Silver Table 
# MAGIC - Spotify Top 50 tracks in Korea

# COMMAND ----------

from pyspark.sql import functions as F 
# bronze -> silver table로 업데이트하면서 processed_time 추가 

country_code = ['kr', 'jp', 'usa']

def update_silver(country):
  query = (spark.readStream
                .table(f"spotify_bronze_{country}")
                .withColumn("processed_time", F.current_timestamp())
                .writeStream.option("checkpointLocation", f"./checkpoints/spotify_silver_{country}")
                .trigger(availableNow=True)
                .table(f"spotify_silver_{country}"))
  query.awaitTermination() # prevent the lessson from moving forward until one batch is processed

for country in country_code:
    update_silver(country)


# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_silver_usa LIMIT 5;
