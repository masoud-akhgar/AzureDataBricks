

dataPath = "dbfs:/mnt/training/definitive-guide/data/activity-data-stream.json"
initialDF = (spark
  .readStream                            # Returns DataStreamReader
  .option("maxFilesPerTrigger", 1)       # Force processing of only 1 file per trigger
  .schema(dataSchema)                    # Required for all streaming DataFrames
  .json(dataPath)                        # The stream's source directory and file type
)


streamingDF = (initialDF
  .withColumnRenamed("Index", "User_ID")  # Pick a "better" column name
  .drop("_corrupt_record")                # Remove an unnecessary column
)

# Static vs Streaming?
streamingDF.isStreaming

from pyspark.sql.functions import col

try:
  sortedDF = streamingDF.orderBy(col("Recorded_At").desc())
  display(sortedDF)
except:
  print("Sorting is not supported on an unaggregated stream")

# COMMAND ----------

basePath = userhome + "/structured-streaming-concepts/python" # A working directory for our streaming app
dbutils.fs.mkdirs(basePath)                                   # Make sure that our working directory exists
outputPathDir = basePath + "/output.parquet"                  # A subdirectory for our output
checkpointPath = basePath + "/checkpoint"                     # A subdirectory for our checkpoint & W-A logs

streamingQuery = (streamingDF                                 # Start with our "streaming" DataFrame
  .writeStream                                                # Get the DataStreamWriter
  .queryName("stream_1p")                                     # Name the query
  .trigger(processingTime="3 seconds")                        # Configure for a 3-second micro-batch
  .format("parquet")                                          # Specify the sink type, a Parquet file
  .option("checkpointLocation", checkpointPath)               # Specify the location of checkpoint files & W-A logs
  .outputMode("append")                                       # Write only new data to the "file"
  .start(outputPathDir)                                       # Start the job, writing to the specified directory
)


streamingQuery.recentProgress


for s in spark.streams.active:         # Iterate over all streams
  print("{}: {}".format(s.id, s.name)) # Print the stream's id and name


streamingQuery.awaitTermination(5)      # Stream for another 5 seconds while the current thread blocks
streamingQuery.stop()                   # Stop the stream
myStream = "stream_2p"
display(streamingDF, streamName = myStream)


print("Looking for {}".format(myStream))

for stream in spark.streams.active:      # Loop over all active streams
  if stream.name == myStream:            # Single out "streamWithTimestamp"
    print("Found {} ({})".format(stream.name, stream.id))

spark.catalog.listTables()


for s in spark.streams.active:
  s.stop()


dbutils.fs.rm(basePath, True)
