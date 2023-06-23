# Databricks notebook source
import dlt 

@dlt.table
def mongodb_gold():
    base_df = dlt.read("mongodb")

    base_df = base_df.withColumn("grade")

    return 
