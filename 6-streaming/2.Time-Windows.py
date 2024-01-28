# Databricks notebook source

inputPath = "dbfs:/mnt/training/sensor-data/accelerometer/time-series-stream.json/"

jsonSchema = "time timestamp, action string"

from pyspark.sql.functions import window, col

inputDF = (spark
  .readStream                                 # Returns an instance of DataStreamReader
  .schema(jsonSchema)                         # Set the schema of the JSON data
  .option("maxFilesPerTrigger", 1)            # Treat a sequence of files as a stream, one file at a time
  .json(inputPath)                            # Specifies the format, path and returns a DataFrame
)

countsDF = (inputDF
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For the aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time and action
)


display(countsDF)


spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

display(countsDF)


for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

watermarkedDF = (inputDF
  .withWatermark("time", "2 hours")           # Specify a 2-hour watermark
  .groupBy(col("action"),                     # Aggregate by action...
           window(col("time"), "1 hour"))     # ...then by a 1 hour window
  .count()                                    # For each aggregate, produce a count
  .select(col("window.start").alias("start"), # Elevate field to column
          col("action"),                      # Include count
          col("count"))                       # Include action
  .orderBy(col("start"), col("action"))       # Sort by the start time
)
display(watermarkedDF)                        # Start the stream and display it

# COMMAND ----------

for s in spark.streams.active: # Iterate over all active streams
  s.stop()                     # Stop the stream

