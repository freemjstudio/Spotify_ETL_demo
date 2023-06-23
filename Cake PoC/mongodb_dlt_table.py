# Databricks notebook source
import dlt 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

    
@dlt.table 
def mongodb_cdc_raw():
    return spark.readStream.format("cloudFiles")\
                .option("header", "true")\
                .option("cloudFiles.format", "csv")\
                .option("escape", '"')\
                .schema("Op STRING, dmsTimestamp TIMESTAMP, _id STRING, _doc STRING")\
                .load("s3://minji-spotify-mongodb/cake/user/")

dlt.create_streaming_live_table(
    name="mongodb",
    comment="Data from DMS for the table: mongodb_cdc_raw"
)

dlt.apply_changes(
    target = "mongodb",
    source = "mongodb_cdc_raw",
    keys = ["_id"],
    sequence_by= col("dmsTimestamp"),
    apply_as_deletes = expr("Op = 'D'"),
    except_column_list = ["Op", "dmsTimestamp"],
    stored_as_scd_type = 1
)
