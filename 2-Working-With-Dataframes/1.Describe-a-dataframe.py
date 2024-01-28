# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Describe a DataFrame
# MAGIC
# MAGIC Your data processing in Azure Databricks is accomplished by defining Dataframes to read and process the Data.
(source, sasEntity, sasToken) = getAzureDataSource()

spark.conf.set(sasEntity, sasToken)


path = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"
files = dbutils.fs.ls(path)
display(files)

parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"


pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetDir)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type

total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))
