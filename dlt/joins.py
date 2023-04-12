# Databricks notebook source
from pyspark.sql.functions import col, from_json, sum, count, current_timestamp, window, expr
from pyspark.sql.types import StructType
import dlt

# COMMAND ----------

@dlt.table()
def impressions_raw():          
    awsAccessKeyId = dbutils.secrets.get("leone", "key")
    awsSecretKey = dbutils.secrets.get("leone", "secret")
    kinesisStreamName = "leone-impressions"
    kinesisRegion = "us-west-2"
    kinesisStreamDf = (
                        spark.readStream
                        .format("kinesis")
                        .option("streamName", kinesisStreamName)
                        .option("region", kinesisRegion)
                        .option("initialPosition", "TRIM_HORIZON")
                        .option("maxRecordsPerFetch", "100")
                        .option("awsAccessKey", awsAccessKeyId)
                        .option("awsSecretKey", awsSecretKey)
                        .load()
                      )
    return kinesisStreamDf

# COMMAND ----------

@dlt.table()
def clicks_raw():          
  awsAccessKeyId = dbutils.secrets.get("leone", "key")
  awsSecretKey = dbutils.secrets.get("leone", "secret")
  kinesisStreamName = "leone-clicks"
  kinesisRegion = "us-west-2"
  kinesisStreamDf = (
                      spark.readStream
                      .format("kinesis")
                      .option("streamName", kinesisStreamName)
                      .option("region", kinesisRegion)
                      .option("initialPosition", "TRIM_HORIZON")
                      .option("maxRecordsPerFetch", "100")
                      .option("awsAccessKey", awsAccessKeyId)
                      .option("awsSecretKey", awsSecretKey)
                      .load()
                    )
  return kinesisStreamDf

# COMMAND ----------

@dlt.table()
def impressions_bronze():
  return (
    dlt.read_stream
    ("impressions_raw").select(from_json(col("data").cast("string"), "adId INT, impressionTime TIMESTAMP").alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in ["adId", "impressionTime"]] + [col("arrival_time"), current_timestamp().alias("processing_time")])
  )

# COMMAND ----------

@dlt.table()
def clicks_bronze():
  return (
    dlt.read_stream("clicks_raw").select(from_json(col("data").cast("string"), "adId INT, clickTime TIMESTAMP").alias("data"), col("approximateArrivalTimestamp").alias("clicks_arrival_time")).select([col(f"data.{c}").alias(c) for c in ["adId", "clickTime"]] + [col("clicks_arrival_time"), current_timestamp().alias("clicks_processing_time")])
  )

# COMMAND ----------

@dlt.table()
def impressions_with_clicks():
  return dlt.read_stream("impressions_bronze").withColumnRenamed("adId", "impressionAdId").withWatermark("impressionTime", "10 seconds").join(dlt.read_stream("clicks_bronze").withColumnRenamed("adId", "clickAdId").withWatermark("clickTime", "20 seconds"),
   expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes
      """
    ), "left")
