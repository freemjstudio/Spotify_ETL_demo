# Databricks notebook source
# MAGIC %md 
# MAGIC ## Load Data From S3

# COMMAND ----------

# load data from S3 
ACCESS_KEY = ""
SECRET_KEY = "/"
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "cake-demo"
MOUNT_FOLDER = "cake"

MOUNT_DIR = "s3a://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME)
dbutils.fs.mount(MOUNT_DIR, f"/mnt/minjiwoo/{MOUNT_FOLDER}") # mount 하기 
display(dbutils.fs.ls(f"/mnt/{MOUNT_FOLDER}"))




# COMMAND ----------

# MAGIC %md 
# MAGIC ## AutoLoader
# MAGIC - read streaming data

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls /dbfs/mnt/minjiwoo/cake

# COMMAND ----------

sparkDF = spark.read.csv("/mnt/minjiwoo/cake/users_interactions.csv", header="true")
sparkDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import * 

bronzeColumns = [
    StructField("timestamp", StringType()),
    StructField("eventType", StringType()),
    StructField("contentId", StringType()),
    StructField("personId", StringType()),
    StructField("sessionId", StringType()),
    StructField("userAgent", StringType()),
    StructField("userRegion", StringType()),
    StructField("userCountry", StringType())
]

# COMMAND ----------

bronzeSchema = StructType(bronzeColumns)

# COMMAND ----------

from pyspark.sql.types import * 

schema2 = StructType().add("timestamp", "string").add("eventType", "string").add("contentId", "string").add("personId", "string").add("sessionId", "string").add("userAgent", "string").add("userRegion", "string").add("userCountry", "string")

# COMMAND ----------

FILE1 = "users_interactions.csv"
FILE2 = "shared_articles.csv"

# COMMAND ----------

df = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .schema(schema2)
    .load("dbfs:/user/minjiwoo@mz.co.kr/mnt/minjiwoo/cake/users_interactions.csv"))

# COMMAND ----------

df.display()

# COMMAND ----------

# write to table 
(df.writeStream.format("delta")
.option("checkpointLocation", "/mnt/minjiwoo/cake/bronze")
.trigger(availableNow=True)
.table("bronze_interactions")) 


# COMMAND ----------

# read + write 합친 함수 

def process_bronze():
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"{DA.paths.checkpoints}/bronze_schema")
                  .load(DA.paths.source_daily)
                  .join(F.broadcast(date_lookup_df), F.to_date((F.col("timestamp")/1000).cast("timestamp")) == F.col("date"), "left")
                  .writeStream
                  .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")
                  .partitionBy("topic", "week_part")
                  .trigger(availableNow=True)
                  .table("bronze"))
 
    query.awaitTermination()

# COMMAND ----------

# MAGIC %md 
# MAGIC - write streaming data
# MAGIC - bronze_interactions

# COMMAND ----------

# DBTITLE 1,Update Silver Table
from pyspark.sql import functions as F 

def update_silver():
    query = (spark.readStream
                    .table("bronze_interaction")
                    .withColumn("processed_time", F.current_timestamp())
                    .writeStream.option("checkpointLocation", "/mnt/minjiwoo/cake/silver")
                    .trigger(availableNow=True)
                    .table("silver_interaction"))
    query.awaitTermination()
update_silver() 
