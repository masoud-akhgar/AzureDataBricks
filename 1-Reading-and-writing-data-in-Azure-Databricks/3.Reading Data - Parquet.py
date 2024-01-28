# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data - Parquet Files
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC - Introduce the Parquet file format.
# MAGIC - Read data from:

parquetFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.parquet/"

(spark.read              # The DataFrameReader
  .parquet(parquetFile)  # Creates a DataFrame from Parquet after reading in the file
  .printSchema()         # Print the DataFrame's schema
)

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

parquetSchema = StructType(
  [
    StructField("timestamp", StringType(), False),
    StructField("site", StringType(), False),
    StructField("requests", IntegerType(), False)
  ]
)

(spark.read               # The DataFrameReader
  .schema(parquetSchema)  # Use the specified schema
  .parquet(parquetFile)   # Creates a DataFrame from Parquet after reading in the file
  .printSchema()          # Print the DataFrame's schema
)

parquetDF = spark.read.schema(parquetSchema).parquet(parquetFile)

print("Partitions: " + str(parquetDF.rdd.getNumPartitions()) )
printRecordsPerPartition(parquetDF)
print("-"*80)

display(parquetDF)
