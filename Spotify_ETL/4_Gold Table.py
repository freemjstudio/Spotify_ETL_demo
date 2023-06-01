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
# MAGIC -- 각 국 가별로 누적된 인기도 + 랭킹 가중치 계산을 통하여 해당 날짜 구간의 차트 만들기 ex) week 단위 
