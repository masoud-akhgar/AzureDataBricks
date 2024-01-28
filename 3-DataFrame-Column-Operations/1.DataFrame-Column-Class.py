# Databricks notebook source

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

parquetFile = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark  # Our SparkSession & Entry Point
  .read                     # Our DataFrameReader
  .parquet(parquetFile)     # Returns an instance of DataFrame
  .cache()                  # cache the data
)
print(pagecountsEnAllDF)


total = pagecountsEnAllDF.count()

print("Record Count: {0:,}".format( total ))


display(pagecountsEnAllDF)


sortedDF = (pagecountsEnAllDF
  .orderBy("requests")
)
sortedDF.show(10, False)

column = col("requests").desc()

# Print the column type
print("column:", column)


sortedDescDF = (pagecountsEnAllDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False) # The top 10 is good enough for now
