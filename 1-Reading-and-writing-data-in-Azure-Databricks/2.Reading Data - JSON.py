# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data - JSON Files
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC - Read data from:

jsonFile = "dbfs:/mnt/training/wikipedia/edits/snapshot-2016-05-26.json"

wikiEditsDF = (spark.read           # The DataFrameReader
    .option("inferSchema", "true")  # Automatically infer data types & column names
    .json(jsonFile)                 # Creates a DataFrame from JSON after reading in the file
 )
wikiEditsDF.printSchema()

from pyspark.sql.types import *

jsonSchema = StructType([
  StructField("channel", StringType(), True),
  StructField("comment", StringType(), True),
  StructField("delta", IntegerType(), True),
  StructField("flag", StringType(), True),
  StructField("geocoding", StructType([
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("countryCode2", StringType(), True),
    StructField("countryCode3", StringType(), True),
    StructField("stateProvince", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True)
  ]), True),
  StructField("isAnonymous", BooleanType(), True),
  StructField("isNewPage", BooleanType(), True),
  StructField("isRobot", BooleanType(), True),
  StructField("isUnpatrolled", BooleanType(), True),
  StructField("namespace", StringType(), True),
  StructField("page", StringType(), True),
  StructField("pageURL", StringType(), True),
  StructField("timestamp", StringType(), True),
  StructField("url", StringType(), True),
  StructField("user", StringType(), True),
  StructField("userURL", StringType(), True),
  StructField("wikipediaURL", StringType(), True),
  StructField("wikipedia", StringType(), True)
])

(spark.read            # The DataFrameReader
  .schema(jsonSchema)  # Use the specified schema
  .json(jsonFile)      # Creates a DataFrame from JSON after reading in the file
  .printSchema()
)

jsonDF = (spark.read
  .schema(jsonSchema)
  .json(jsonFile)    
)
print("Partitions: " + str(jsonDF.rdd.getNumPartitions()))
printRecordsPerPartition(jsonDF)
print("-"*80)

display(jsonDF)
