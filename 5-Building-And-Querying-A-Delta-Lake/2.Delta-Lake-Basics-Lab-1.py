# Databricks notebook source

DeltaPath = userhome + "/delta/customer-data/"


deltaDF = (spark.read
  FILL_IN

customerCounts = (deltaDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders"))

display(customerCounts)

CustomerCountsPath = userhome + "/delta/customer_counts/"

dbutils.fs.rm(CustomerCountsPath, True) #deletes Delta table if previously created

newDataPath = "/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv"
newDataDF = (spark
  .read
  .option("header", "true")
  .schema(inputSchema)
  .csv(newDataPath)
)


newCustomerCounts = (newDataDF.groupBy("CustomerID", "Country")
  .count()
  .withColumnRenamed("count", "total_orders"))

# COMMAND ----------

display(newCustomerCounts)

newCustomerCounts.createOrReplaceTempView("new_customer_counts")


(newDataDF.write
  .format("delta")
  .mode("append")
  .save(DeltaPath))
