# Databricks notebook source
# MAGIC %md 
# MAGIC ## Silver Table 
# MAGIC - Spotify Top 50 tracks in Korea

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_bronze_KR LIMIT 10;

# COMMAND ----------

from pyspark.sql import functions as F 
# bronze -> silver table로 업데이트하면서 processed_time 추가 

def update_silver():
  query = (spark.readStream
                .table("spotify_bronze")
                .withColumn("processed_time", F.current_timestamp())
                .withColumn("playlist_nation", "Korea")
                .writeStream.option("checkpointLocation", f"{DA.paths.checkpoints}/spotify_silver_KR")
                .trigger(availableNow=True)
                .table("spotify_silver_KR"))
  query.awaitTermination() # prevent the lessson from moving forward until one batch is processed

update_silver()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_KR_silver LIMIT 5;
