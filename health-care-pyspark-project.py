# Databricks notebook source
spark

# COMMAND ----------

# Performing necessary imports required for the project
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Function for defining Struct type for different datasets
def define_schemas():    

    # Schema for Encounters Dataset
    encounters_schema = StructType([
        StructField("id", StringType(), True),
        StructField("start", TimestampType(), True),
        StructField("stop", TimestampType(), True),
        StructField("patient", StringType(), True),
        StructField("organization", StringType(), True),
        StructField("provider", StringType(), True),
        StructField("payer", StringType(), True),
        StructField("encounterclass", StringType(), True),
        StructField("code", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("base_encounter_cost", DoubleType(), True),
        StructField("total_claim_cost", DoubleType(), True),
        StructField("payer_coverage", DoubleType(), True),
        StructField("reasoncode", StringType(), True)
    ])

    # Schema for Patient Dataset
    patients_schema = StructType([
        StructField("id", StringType(), True),
        StructField("birthdate", DateType(), True),
        StructField("deathdate", DateType(), True),
        StructField("ssn", StringType(), True),
        StructField("drivers", StringType(), True),
        StructField("passport", StringType(), True),
        StructField("prefix", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
        StructField("suffix", StringType(), True),
        StructField("maiden", StringType(), True),
        StructField("marital", StringType(), True),
        StructField("race", StringType(), True),
        StructField("ethnicity", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("birthplace", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("county", StringType(), True),
        StructField("fips", IntegerType(), True),
        StructField("zip", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("healthcare_expenses", DoubleType(), True),
        StructField("healthcare_coverage", DoubleType(), True),
        StructField("income", IntegerType(), True),
        StructField("mrn", IntegerType(), True)
    ])

    # Schema for Conditions Dataset
    conditions_schema = StructType([
    StructField("start", TimestampType(),True),
    StructField("stop", TimestampType(),True),
    StructField("patient", StringType(),True),
    StructField("encounter", StringType(),True),
    StructField("code", StringType(),True),
    StructField("description", StringType(),True),
    ])


    # Schema for Immunization Dataset
    immunizations_schema = StructType([
        StructField("date", TimestampType(), True),
        StructField("patient", StringType(), True),
        StructField("encounter", StringType(), True),
        StructField("code", IntegerType(), True),
        StructField("description", StringType(), True)
    ])
    
    return patients_schema, encounters_schema, conditions_schema, immunizations_schema






# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Extraction

# COMMAND ----------

def extract_data(spark, patients_schema, encounters_schema, conditions_schema, immunizations_schema):
    encounters_df = spark.read.csv("s3://health-care-data-bucket/encounters.csv", schema=encounters_schema, header=True)
    patients_df = spark.read.csv("s3://health-care-data-bucket/patients.csv", schema=patients_schema, header=True)
    conditions_df = spark.read.csv("s3://health-care-data-bucket/conditions.csv", schema=conditions_schema, header=True)
    immunizations_df = spark.read.csv("s3://health-care-data-bucket/immunizations.csv", schema=immunizations_schema, header=True)
    return patients_df, encounters_df, conditions_df, immunizations_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning and Transformation

# COMMAND ----------

def transform_data(patients_df, encounters_df, conditions_df, immunizations_df):

# Cleaning and Few Transformation PATIENT DATA 
    clean_patients_df = patients_df\
        .withColumn("gender", when(col("gender") == "M", "Male")\
                    .when(col("gender") == "F", "Female")\
                    .otherwise("Unknown"))\
        .withColumn("marital", when(col("marital") == "M", "Married")\
                    .when(col("marital") == "S", "Single")\
                    .when(col("marital") == "D", "Divorced")\
                    .otherwise("Unknown"))\
        .withColumnRenamed("id", "patient_id")\
        .withColumnRenamed("first", "first_name")\
        .withColumnRenamed("last", "last_name")\
        .withColumnRenamed("lat","latitude")\
        .withColumnRenamed("lon","longitutde")
    # clean_patients_df.limit(10).display()

# Cleaning and Few Transformation IMMUNIZATION DATA 
    clean_immunization_df = immunizations_df\
        .withColumn("date", immunizations_df["date"].cast("date")).withColumnRenamed('patient','vaccined_patient_id')\
        .withColumnRenamed("encounter","encounter_id")\
        .withColumnRenamed("code","vaccine_code")

# Cleaning and Few Transformation ENCOUNTERS DATA 
    #Start Date and Time Seperation
    clean_immunization_df = encounters_df\
        .withColumn("start_time", date_format('start', 'HH:mm:ss') )\
        .withColumn("start", encounters_df['start'].cast('date'))\
        .withColumn("end_time", date_format('stop', 'HH:mm:ss'))\
        .withColumn("stop", encounters_df['stop'].cast('date'))\
        .withColumnRenamed('id','encounter_id')\
        .withColumnRenamed('start','start_date')\
        .withColumnRenamed('stop','stop_date')\
        .withColumnRenamed('patient','patient_id')\
        .withColumnRenamed('code','encounter_code')

    # clean_immunization_df.limit(10).display()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Code Execution

# COMMAND ----------


def execute():
    spark = SparkSession.builder.appName("Healthcare_ETL").getOrCreate()
    patients_schema, encounters_schema, conditions_schema, immunizations_schema = define_schemas()
    patients_df, encounters_df, conditions_df, immunizations_df = extract_data(spark, patients_schema, encounters_schema, conditions_schema, immunizations_schema )
    transform_data(patients_df, encounters_df, conditions_df, immunizations_df)

execute()

# COMMAND ----------

    

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


