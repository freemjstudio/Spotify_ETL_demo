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

# MAGIC %md 
# MAGIC ### Drop Duplicates

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Upsert Silver Table with Microbatch Function 

# COMMAND ----------

sql_query = """
    MERGE INTO spotify_silver_kr a 
    USING stream_updates b 
    ON a.song=b.song AND a.date=b.date
    WHEN NOT MATCHED THEN INSERT *
"""
# song + date 조합을 복합키처럼 사용 

# COMMAND ----------

class Upsert:
    def __init__(self, sql_query, update_temp="stream_updates"):
        self.sql_query = sql_query
        self.update_temp = update_temp

    def upsert(self, microBatchDF, batch):
        microBatchDF.createOrRepalceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

# COMMAND ----------

streaming_merge = Upsert(sql_query) # upsert 객체 

# COMMAND ----------

streaming_df_kr = (spark.readStream
                        .table("spotify_bronze_kr")
                        .select("*")
                        .withWatermark("time", "30 seconds") 
                        .dropDuplicates()
                        )
streaming_df_kr.display()
# when to close the aggregate windows and produce the aggregate result.

# COMMAND ----------

from pyspark.sql import functions as F 
# bronze -> silver table로 업데이트하면서 processed_time 추가 

country_code = ['kr', 'jp', 'usa']

def update_silver(country):
  query = (spark.readStream
                .table(f"spotify_bronze_{country}")
                .withColumn("processed_time", F.current_timestamp())
                .dropDuplicates()  
                .writeStream
                .foreachBatch(streaming_merge.upsert)
                .outputMode("append")
                .option("checkpointLocation", f"./checkpoints/spotify_silver_{country}")
                .trigger(availableNow=True)
                .table(f"spotify_silver_{country}"))
  query.awaitTermination() # prevent the lessson from moving forward until one batch is processed

for country in country_code:
    update_silver(country)

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
                .table(f"spotify_silver_{country}"))
  query.awaitTermination() # prevent the lessson from moving forward until one batch is processed

for country in country_code:
    update_silver(country)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Check Silver Table Quality

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_silver_kr LIMIT 10;

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/user/hive/warehouse/spotify_bronze_kr

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd dbfs:/user/hive/warehouse/spotify_bronze_kr

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_silver_jp;
