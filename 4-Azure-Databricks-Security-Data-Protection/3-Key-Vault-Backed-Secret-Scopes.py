# Databricks notebook source

salesDF = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .csv(MOUNTPOINT + "/sales.csv"))

display(salesDF)


from pyspark.sql.functions import col

sales2004DF = (salesDF
                  .filter((col("ShipDateKey") > 20031231) &
                          (col("ShipDateKey") <= 20041231)))
display(sales2004DF)

try:
  sales2004DF.write.mode("overwrite").parquet(MOUNTPOINT + "/sales2004")
except Exception as e:
  print(e)


sales2004DF.write.mode("overwrite").parquet(SOURCE + "/sales2004")

dbutils.fs.rm(SOURCE + "/sales2004", True)
