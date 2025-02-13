# Databricks notebook source
spark

# COMMAND ----------

# importing Spark session from pyspark.sql
from pyspark.sql import SparkSession 

# Creating Spark Session with Appname
spark = SparkSession.builder.appName("Health Care Data Engineering Spark Project").getOrCreate()

# getOrCreate() this will get the app if created or else it will create a new session

# COMMAND ----------

# Read Health Data CSV Files from AWS S3 Bucket
conditions_df = spark.read.format("csv").option("header","true").load("s3://health-care-data-bucket/conditions.csv")

# initially AWS S3 Bucket was not public so I changed the bucket policy changed all the block settings from the AWS Console

# COMMAND ----------

conditions_df.show(5)

# COMMAND ----------


