# Databricks notebook source
# MAGIC %md
# MAGIC # Writing Data
# MAGIC
# MAGIC Just as there are many ways to read data, we have just as many ways to write data.
# MAGIC
# MAGIC In this notebook, we will take a quick peek at how to write data back out to Parquet files.
# MAGIC **Technical Accomplishments:**
# MAGIC - Writing data to Parquet files


from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])

csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)


fileName = userhome + "/pageviews_by_second.parquet"
print("Output location: " + fileName)

(csvDF.write                       # Our DataFrameWriter
  .option("compression", "snappy") # One of none, snappy, gzip, and lzo
  .mode("overwrite")               # Replace existing files
  .parquet(fileName)               # Write DataFrame to Parquet files
)


display(
  dbutils.fs.ls(fileName)
)


display(
  spark.read.parquet(fileName)
)

