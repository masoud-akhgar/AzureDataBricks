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


from pyspark.sql.functions import *

sortedDescDF = (pagecountsEnAllDF
  .orderBy( col("requests").desc() )
)  
sortedDescDF.show(10, False)


whereDF = (sortedDescDF
  .where( "project = 'en'" )
)
whereDF.show(10, False)


filteredDF = (sortedDescDF
  .filter( col("project") == "en")
)
filteredDF.show(10, False)

articlesDF = (filteredDF
  .drop("bytes_served")
  .filter( col("article") != "Main_Page")
  .filter( col("article") != "-")
  .filter( col("article").startswith("Special:") == False)
)
articlesDF.show(10, False)


firstRow = articlesDF.first()

print(firstRow)

article = firstRow['article']
total = firstRow['requests']

print("Most Requested Article: \"{0}\" with {1:,} requests".format( article, total ))


rows = (articlesDF
  .limit(10)           # We only want the first 10 records.
  .collect()           # The action returning all records in the DataFrame
)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)

rows = articlesDF.take(10)

# rows is an Array. Now in the driver, 
# we can just loop over the array and print 'em out.

listItems = ""
for row in rows:
  project = row['article']
  total = row['requests']
  listItems += "    <li><b>{}</b> {:0,d} requests</li>\n".format(project, total)
  
html = """
<body>
  <h1>Top 10 Articles</h1>
  <ol>
    %s
  </ol>
</body>
""" % (listItems.strip())

print(html)
