# Databricks notebook source
# MAGIC %md
# MAGIC ### Gold Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_silver_JP LIMIT 5;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM spotify_silver_KR LIMIT 5;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT count(*) FROM spotify_silver_USA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 한국 일본 합쳐서 daily top 50 순위의 차트 만들기 
# MAGIC -- rank 순위 , popularity , release date 최신순 순서대로 만들기 

# COMMAND ----------

# weekly 차트 만들기 
