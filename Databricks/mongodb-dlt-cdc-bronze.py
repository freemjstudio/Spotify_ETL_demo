# Databricks notebook source
# import dlt 
# from pyspark.sql.functions import * 
# from pyspark.sql.types import * 

# schema = StructType()

# @dlt.table 
# def mongodb_cdc_raw():
#     return spark.readStream.format("cloudFiles")\
#                 .option("header", "true")\
#                 .option("cloudFiles.format", "csv")\
#                 .option("escape", '"')\
#                 .schema("Op STRING, dmsTimestamp TIMESTAMP, _id STRING, _doc STRING")\
#                 .load("s3://minji-spotify-mongodb/cake/user/")

# dlt.create_streaming_live_table(
#     name="mongodb",
#     comment="Data from DMS for the table: mongodb_cdc_raw"
# )

# dlt.apply_changes(
#     target = "mongodb",
#     source = "mongodb_cdc_raw",
#     keys = ["_id"],
#     sequence_by= col("dmsTimestamp"),
#     apply_as_deletes = expr("Op = 'D'"),
#     except_column_list = ["Op", "dmsTimestamp"],
#     stored_as_scd_type = 1
# )



# COMMAND ----------

# import dlt 
# from pyspark.sql.functions import * 
# from pyspark.sql.types import * 

# @dlt.table 
# def mongodb_silver():
#     return spark.sql(
#         '''
#         SELECT from_json(_doc, '_id STRING, user_id STRING, age INT')._id as _id,
#         from_json(_doc, '_id STRING, user_id STRING, age INT').user_id as user_id,
#         from_json(_doc, '_id STRING, user_id STRING, age INT').age as age
#         FROM live.mongodb     
#         ''')
    

# COMMAND ----------

import dlt 
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BinaryType


# schema = StructType([
#     StructField('Op', StringType()),
#     StructField('dmsTimestamp', TimestampType()),
#     StructField('_id', StringType()),
#     StructField("_doc", 
#                 StructType([
#                     StructField('_id', MapType(StringType(),StringType()), True),
#                     StructField('user_id', StringType(), True),
#                     StructField('age', LongType(), True)
#                 ])
#                 )])


@dlt.table 
def mongodb_cdc_raw2():
    return spark.readStream.format("cloudFiles")\
                .option("header", "true")\
                .option("cloudFiles.format", "csv")\
                .option("escape", '"')\
                .schema(schema)\
                .load("s3://minji-spotify-mongodb/cake/user/")

dlt.create_streaming_live_table(
    name="mongodb2",
    comment="Data from DMS for the table: mongodb_cdc_raw"
)

dlt.apply_changes(
    target = "mongodb2",
    source = "mongodb_cdc_raw2",
    keys = ["_id"],
    sequence_by= col("dmsTimestamp"),
    apply_as_deletes = expr("Op = 'D'"),
    except_column_list = ["Op", "dmsTimestamp"],
    stored_as_scd_type = 1
)

