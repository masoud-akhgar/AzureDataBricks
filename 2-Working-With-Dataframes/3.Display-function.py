# Databricks notebook source


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


pagecountsEnAllDF.show()

display(pagecountsEnAllDF)


limitedDF = pagecountsEnAllDF.limit(5) # "limit" the number of records to the first 5

limitedDF # Python hack to force printing of the data type


limitedDF.show(100, False) #show up to 100 records and don't truncate the columns


display(limitedDF) # defaults to the first 1000 records


pagecountsEnAllDF.printSchema()

onlyThreeDF = (pagecountsEnAllDF
  .select("project", "article", "requests") # Our 2nd transformation (4 >> 3 columns)
)
onlyThreeDF.printSchema()

onlyThreeDF.show(5, False)

droppedDF = (pagecountsEnAllDF
  .drop("bytes_served") # Our second transformation after the initial read (4 columns down to 3)
)
# Now let's take a look at what the schema looks like
droppedDF.printSchema()

droppedDF.show(5, False)


pagecountsEnAllDF.printSchema()


distinctDF = (pagecountsEnAllDF     # Our original DataFrame from spark.read.parquet(..)
  .select("project")                # Drop all columns except the "project" column
  .distinct()                       # Reduce the set of all records to just the distinct column.
)

distinctDF.show(100, False)               

total = distinctDF.count()     
print("Distinct Projects: {0:,}".format( total ))


parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"

pagecountsEnAllDF = (spark       # Our SparkSession & Entry Point
  .read                          # Our DataFrameReader
  .parquet(parquetDir)           # Returns an instance of DataFrame
)
(pagecountsEnAllDF               # Only if we are running multiple queries
  .cache()                       # mark the DataFrame as cachable
  .count()                       # materialize the cache
)
distinctDF = (pagecountsEnAllDF  # Our original DataFrame from spark.read.parquet(..)
  .select("project")             # Drop all columns except the "project" column
  .distinct()                    # Reduce the set of all records to just the distinct column.
)
total = distinctDF.count()     
print("Distinct Projects: {0:,}".format( total ))

pagecountsEnAllDF.createOrReplaceTempView("pagecounts")


tableDF = spark.sql("SELECT DISTINCT project FROM pagecounts ORDER BY project")
display(tableDF)
