# Databricks notebook source


DeltaPath = userhome + "/delta/customer-data/"
CustomerCountsPath = userhome + "/delta/customer_counts/"

# ANSWER
count = spark.sql("SELECT SUM(total_orders) FROM customer_counts WHERE Country='Sweden'").collect()[0][0]

print(count)

%timeit preZorderQuery = spark.sql("SELECT * FROM customer_data_delta WHERE StockCode=22301 ").collect()

%timeit postZorderQuery = spark.sql("SELECT * FROM customer_data_delta WHERE StockCode=22301").collect()

preNumFiles = len(dbutils.fs.ls(DeltaPath + "/Country=Sweden"))
print(preNumFiles)


spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

postNumFiles = len(dbutils.fs.ls(DeltaPath + "/Country=Sweden"))
print(postNumFiles)
