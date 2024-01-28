# Databricks notebook source
# # Reading Data - CSV Files
# **Technical Accomplishments:**
# - Start working with the API documentation
# - Introduce the class `SparkSession` and other entry points
# - Introduce the class `DataFrameReader`
# - Read data from:

# %md
# ##![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Getting Started
# MAGIC
# Run the following cell to configure our "classroom."

# %run "./Includes/Classroom-Setup"

# COMMAND ----------

# %run "./Includes/Utility-Methods"

print(spark)
from pyspark.sql import SparkSession
dfreader = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()

lines = dfreader.read.jdbc("https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/03-Reading-and-writing-data-in-Azure-Databricks.dbc?raw=true")
print(dfreader.jdbc("https://github.com/solliancenet/microsoft-learning-paths-databricks-notebooks/blob/master/data-engineering/DBC/03-Reading-and-writing-data-in-Azure-Databricks.dbc?raw=true"))

print(sc)
print(sqlContext)

# COMMAND -
# Quick function review:
# * `createDataSet(..)`
# * `createDataFrame(..)`
# * `emptyDataSet(..)`
# * `emptyDataFrame(..)`
# * `range(..)`
# * `read(..)`
# * `readStream(..)`
# * `sparkContext(..)`
# * `sqlContext(..)`
# * `sql(..)`
# * `streams(..)`
# * `table(..)`
# * `udf(..)`
# Quick function review:
# * `csv(path)`
# * `jdbc(url, table, ..., connectionProperties)`
# * `json(path)`
# * `format(source)`
# * `load(path)`
# * `orc(path)`
# * `parquet(path)`
# * `table(tableName)`
# * `text(path)`
# * `textFile(path)`
# MAGIC
# Configuration methods:
# * `option(key, value)`
# * `options(map)`
# * `schema(schema)`

csvFile = "/mnt/training/wikipedia/pageviews/pageviews_by_second.tsv"
tempDF = (spark.read           # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
)

tempDF.printSchema()


display(tempDF)

(spark.read                    # The DataFrameReader
   .option("sep", "\t")        # Use tab delimiter (default is comma-separator)
   .option("header", "true")   # Use first line of all files as header
   .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

(spark.read                        # The DataFrameReader
   .option("header", "true")       # Use first line of all files as header
   .option("sep", "\t")            # Use tab delimiter (default is comma-separator)
   .option("inferSchema", "true")  # Automatically infer data types
   .csv(csvFile)                   # Creates a DataFrame from CSV after reading in the file
   .printSchema()
)

# Required for StructField, StringType, IntegerType, etc.
from pyspark.sql.types import *

csvSchema = StructType([
  StructField("timestamp", StringType(), False),
  StructField("site", StringType(), False),
  StructField("requests", IntegerType(), False)
])
# ### Step #2
# Read in our data (and print the schema).
# MAGIC
# We can specify the schema, or rather the `StructType`, with the `schema(..)` command:

(spark.read                   # The DataFrameReader
  .option('header', 'true')   # Ignore line #1 - it's a header
  .option('sep', "\t")        # Use tab delimiter (default is comma-separator)
  .schema(csvSchema)          # Use the specified schema
  .csv(csvFile)               # Creates a DataFrame from CSV after reading in the file
  .printSchema()
)

csvDF = (spark.read
  .option('header', 'true')
  .option('sep', "\t")
  .schema(csvSchema)
  .csv(csvFile)
)
print("Partitions: " + str(csvDF.rdd.getNumPartitions()) )
printRecordsPerPartition(csvDF)
print("-"*80)
