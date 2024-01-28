# Databricks notebook source

(source, sasEntity, sasToken) = getAzureDataSource()
spark.conf.set(sasEntity, sasToken)

parquetDir = source + "/wikipedia/pagecounts/staging_parquet_en_only_clean/"


from pyspark.sql.functions import *

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

washingtons = (spark.read
  .parquet(parquetDir)
  .filter( col("project") == "en")
  .filter( col("article").endswith("_Washington") )
  #.filter( col("article").like("%\\_Washington") )
  .collect()
  #.take(1000)
)
totalWashingtons = 0

for washington in washingtons:
  totalWashingtons += washington["requests"]

print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

from pyspark.sql.functions import *  # sum(), count()

parquetDir = "/mnt/training/wikipedia/pagecounts/staging_parquet_en_only_clean/"

stats = (spark.read
  .parquet(parquetDir)
  .filter((col("project") == "en") & col("article").endswith("_Washington"))
  .select(sum("requests"), count("*"))
  .first())

totalWashingtons = stats[0]
washingtonCount = stats[1]

print("Total Washingtons: {}".format(washingtonCount) )
print("Total Washingtons Requests: {}".format(totalWashingtons))

# COMMAND ----------

# ANSWER

from pyspark.sql.functions import *

marthas = (spark.read
  .parquet(parquetDir)
  .filter( col("project") == "en")
  #.filter( col("article").startswith("Martha_") )
  .filter( col("article").like("Martha\\_%") )
  #.collect()
  .take(1000)
)
totalMarthas = 0

for martha in marthas:
  totalMarthas += martha["requests"]

print("Total Marthas: {0:,}".format( len(marthas) ))
print("Total Marthas Requests: {0:,}".format( totalMarthas ))

# COMMAND ----------


print("Total Washingtons: {0:,}".format( len(washingtons) ))
print("Total Washington Requests: {0:,}".format( totalWashingtons ))

expectedCount = 466
assert len(washingtons) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(washingtons) )

expectedTotal = 3266
assert totalWashingtons == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalWashingtons)

# COMMAND ----------

print("Total Marthas: {0:,}".format( len(marthas) ))
print("Total Marthas Requests: {0:,}".format( totalMarthas ))

expectedCount = 146
assert len(marthas) == expectedCount, "Expected " + str(expectedCount) + " articles but found " + str( len(marthas) )

expectedTotal = 708
assert totalMarthas == expectedTotal, "Expected " + str(expectedTotal) + " requests but found " + str(totalMarthas)
