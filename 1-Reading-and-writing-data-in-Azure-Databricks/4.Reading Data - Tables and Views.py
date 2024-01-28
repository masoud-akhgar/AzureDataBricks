# Databricks notebook source
# MAGIC %md
# MAGIC # Reading Data - Tables and Views
# MAGIC
# MAGIC **Technical Accomplishments:**
# MAGIC * Demonstrate how to pre-register data sources in Azure Databricks.
# MAGIC * Introduce temporary views over files.
# MAGIC * Read data from tables/views.
# MAGIC * Regarding `printRecordsPerPartition(..)`, it 
pageviewsBySecondsExampleDF = spark.read.table("pageviews_by_second_example_tsv")

pageviewsBySecondsExampleDF.printSchema()


display(pageviewsBySecondsExampleDF)


print("Partitions: " + str(pageviewsBySecondsExampleDF.rdd.getNumPartitions()))
printRecordsPerPartition(pageviewsBySecondsExampleDF)
print("-"*80)

# create a DataFrame from a parquet file
parquetFile = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"
parquetDF = spark.read.parquet(parquetFile)

# create a temporary view from the resulting DataFrame
parquetDF.createOrReplaceTempView("parquet_table")
