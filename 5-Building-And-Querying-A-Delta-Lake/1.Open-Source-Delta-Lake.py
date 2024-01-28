# Databricks notebook source


inputPath = "/mnt/training/online_retail/data-001/data.csv"
DataPath = userhome + "/delta/customer-data/"

#remove directory if it exists
dbutils.fs.rm(DataPath, True)

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

inputSchema = StructType([
  StructField("InvoiceNo", IntegerType(), True),
  StructField("StockCode", StringType(), True),
  StructField("Description", StringType(), True),
  StructField("Quantity", IntegerType(), True),
  StructField("InvoiceDate", StringType(), True),
  StructField("UnitPrice", DoubleType(), True),
  StructField("CustomerID", IntegerType(), True),
  StructField("Country", StringType(), True)
])

rawDataDF = (spark.read
  .option("header", "true")
  .schema(inputSchema)
  .csv(inputPath)
)

# write to Delta Lake
rawDataDF.write.mode("overwrite").format("delta").partitionBy("Country").save(DataPath)

display(spark.sql("SELECT * FROM delta.`{}` LIMIT 5".format(DataPath)))


spark.sql("""
  DROP TABLE IF EXISTS customer_data_delta
""")
spark.sql("""
  CREATE TABLE customer_data_delta
  USING DELTA
  LOCATION '{}'
""".format(DataPath))


display(dbutils.fs.ls(DataPath + "/_delta_log"))


miniDataInputPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv"

newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(miniDataInputPath)
)


newDataDF.count()

(newDataDF
  .write
  .format("delta")
  .partitionBy("Country")
  .mode("append")
  .save(DataPath)
)


upsertDF = spark.read.format("json").load("/mnt/training/enb/commonfiles/upsert-data.json")
display(upsertDF)

upsertDF.createOrReplaceTempView("upsert_data")
