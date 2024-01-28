# Databricks notebook source


spark.sql("""
    DROP TABLE IF EXISTS iot_data
  """)
spark.sql("""
    CREATE TABLE iot_data
    USING DELTA
    LOCATION '{}/delta/iot-events/'
  """.format(userhome))


iotPath = userhome + "/delta/iot-events/"


display(dbutils.fs.ls(iotPath + "/date=2016-07-26"))


len(dbutils.fs.ls(iotPath + "date=2016-07-26"))


spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)


len(dbutils.fs.ls(iotPath + "date=2016-07-26"))
