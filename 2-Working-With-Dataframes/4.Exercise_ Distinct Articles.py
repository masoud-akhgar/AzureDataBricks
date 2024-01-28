# Databricks notebook source

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

from pyspark.sql.types import *

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

df = (spark
  .read
  .parquet(parquetDir)
  .select("article")
  .distinct()
)
totalArticles = df.count()

print("Distinct Articles: {0:,}".format( totalArticles ))

from pyspark.sql.types import *

schema = StructType([
  StructField("project", StringType(), False),
  StructField("article", StringType(), False),
  StructField("requests", IntegerType(), False),
  StructField("bytes_served", LongType(), False)
])

totalArticles = (spark.read
  .schema(schema)
  .parquet(parquetDir)
  .select("article")
  .distinct()
  .count()
)

print("Distinct Articles: {0:,}".format( totalArticles ))


expected = 1783138
assert totalArticles == expected, "Expected the total to be " + str(expected) + " but found " + str(totalArticles)
