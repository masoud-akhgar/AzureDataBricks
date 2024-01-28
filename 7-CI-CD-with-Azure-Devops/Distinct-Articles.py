# Databricks notebook source

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"


from pyspark.sql.types import *

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

schema = StructType([
  StructField("project", StringType(), False),
  StructField("article", StringType(), False),
  StructField("requests", IntegerType(), False),
  StructField("bytes_served", LongType(), False)
])

df = (spark.read
  .schema(schema)
  .parquet(parquetDir)
  .select("*")
  .distinct()
)

totalArticles = df.count()

print("Distinct Articles: {0:,}".format( totalArticles ))

display(df)
