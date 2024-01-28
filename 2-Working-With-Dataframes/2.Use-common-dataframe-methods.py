# Databricks notebook source

(source, sasEntity, sasToken) = getAzureDataSource()

spark.conf.set(sasEntity, sasToken)


parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetDir)      # Returns an instance of DataFrame
)
print(pagecountsEnAllDF)    # Python hack to see the data type


total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))


(pagecountsEnAllDF
  .cache()         # Mark the DataFrame as cached
  .count()         # Materialize the cache
) 

pagecountsEnAllDF.count()


pagecountsEnAllDF.printSchema()
