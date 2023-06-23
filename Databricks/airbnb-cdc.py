# Databricks notebook source
import dlt 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


@dlt.table 
def airbnb_cdc_raw():
    return spark.readStream.format("cloudFiles")\
                .option("header", "true")\
                .option("cloudFiles.format", "csv")\
                .option("escape", '"')\
                .option("cloudFiles.schemaLocation", "s3://minji-spotify-mongodb/checkpoint")\
                .load("s3://minji-spotify-mongodb/cake/airbnb/")
    

dlt.create_streaming_live_table(
    name="airbnb",
    comment="Data from DMS for the table: airbnb_cdc_raw"
)

dlt.apply_changes(
    target = "airbnb",
    source = "airbnb_cdc_raw",
    keys = ["_id"],
    sequence_by= col("dmsTimestamp"),
    apply_as_deletes = expr("Op = 'D'"),
    except_column_list = ["Op", "dmsTimestamp"],
    stored_as_scd_type = 1
)

# COMMAND ----------

@dlt.table
def airbnb_silver():
    return spark.readStream.format("cloudFiles")\
        .option
