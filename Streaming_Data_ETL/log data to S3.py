# Databricks notebook source
from io import StringIO # python3; python2: BytesIO 
import boto3

bucket = 'my_bucket_name' # already created on S3
csv_buffer = StringIO()
df.to_csv(csv_buffer)
s3_resource = boto3.resource('s3')
s3_resource.Object(bucket, 'df.csv').put(Body=csv_buffer.getvalue())
