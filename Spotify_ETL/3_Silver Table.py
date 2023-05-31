# Databricks notebook source
# MAGIC %md 
# MAGIC ## Silver Table 
# MAGIC - Spotify Top 50 tracks in Korea

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE spotify_bronze_kr;

# COMMAND ----------

bronze_kr_count = (spark.read
                  .table("spotify_bronze_kr")
                  .count()
                  )
print("total_bronze:", bronze_kr_count)


# COMMAND ----------

silver_kr_count = (spark.read
                  .table("spotify_silver_kr")
                  .count()
                  )
print("total_silver:", silver_kr_count)

# COMMAND ----------

sql_query = """
    MERGE INTO spotify_silver_kr a 
    USING stream_updates b 
    ON a.id=b.id AND a.date=b.date
    WHEN NOT MATCHED THEN INSERT *
"""

# COMMAND ----------

class Upsert:
    def __init__():
        self.
    
    def upsert()

# COMMAND ----------

streaming_merge = Upsert(sql_query)

# COMMAND ----------

from pyspark.sql import functions as F 
# bronze -> silver table로 업데이트하면서 processed_time 추가 

country_code = ['kr', 'jp', 'usa']

def update_silver(country):
  query = (spark.readStream
                .table(f"spotify_bronze_{country}")
                .withColumn("processed_time", F.current_timestamp())
                .writeStream
                .option("checkpointLocation", f"./checkpoints/spotify_silver_{country}")
                .trigger(availableNow=True)
                .outputMode("append")
                .table(f"spotify_silver_{country}"))
  query.awaitTermination() # prevent the lessson from moving forward until one batch is processed

for country in country_code:
    update_silver(country)


# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/user/hive/warehouse/spotify_bronze_kr

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd dbfs:/user/hive/warehouse/spotify_bronze_kr

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_silver_jp;

# COMMAND ----------


