# Databricks notebook source
# MAGIC %run ../StreamJoin/StreamJoin

# COMMAND ----------

from pyspark.sql.functions import col, from_json, sum, count, current_timestamp, window, expr
from pyspark.sql.types import StructType

# COMMAND ----------

import pyspark.sql.types
from pyspark.sql.types import _parse_datatype_string

def toDDL(self):
    """
    Returns a string containing the schema in DDL format.
    """
    from pyspark import SparkContext
    sc = SparkContext._active_spark_context
    dt = sc._jvm.__getattr__("org.apache.spark.sql.types.DataType$").__getattr__("MODULE$")
    json = self.json()
    return dt.fromJson(json).toDDL()
pyspark.sql.types.DataType.toDDL = toDDL
pyspark.sql.types.StructType.fromDDL = _parse_datatype_string

# COMMAND ----------

# Enable auto compaction and optimized writes in Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)
spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", "2GB")

# COMMAND ----------

dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/cp", True)
dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/cp", True)
dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks_for_impressions/cp", True)

dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/data", True)
dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/data", True)
dbutils.fs.rm("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks_for_impressions/data", True)

# COMMAND ----------

schemaDict = {}
schema = StructType.fromDDL("adId INT, impressionTime TIMESTAMP")
cols = [f.name for f in schema.fields]
schemaDict["leone-impressions"] = (schema, cols)
schema = StructType.fromDDL("adId INT, clickTime TIMESTAMP")
cols = [f.name for f in schema.fields]
schemaDict["leone-clicks"] = (schema, cols)

# COMMAND ----------

def readStream(stream_name):
  spark.conf.set("spark.sql.shuffle.partitions", 32)
  awsAccessKeyId = dbutils.secrets.get("leone", "key")
  awsSecretKey = dbutils.secrets.get("leone", "secret")
  kinesisStreamName = stream_name
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

impStreamDf = readStream("leone-impressions").select(from_json(col("data").cast("string"), schemaDict["leone-impressions"][0]).alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in schemaDict["leone-impressions"][1]] + [col("arrival_time"), current_timestamp().alias("processing_time")])

# COMMAND ----------

clickStreamDf = readStream("leone-clicks").select(from_json(col("data").cast("string"), schemaDict["leone-clicks"][0]).alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in schemaDict["leone-clicks"][1]] + [col("arrival_time"), current_timestamp().alias("processing_time")])

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE  IF NOT EXISTS delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/data` (adId INT, impressionTime TIMESTAMP, arrival_time TIMESTAMP, processing_time TIMESTAMP) USING delta

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "impressions")
(impStreamDf
  .writeStream
  .format('delta')
  .queryName("impressions")
  .option('checkpointLocation', "/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/cp")
  .start("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/data"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/data` (adId INT, clickTime TIMESTAMP, click_arrival_time TIMESTAMP, click_processing_time TIMESTAMP) USING delta

# COMMAND ----------

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "clicks")
(clickStreamDf.withColumnRenamed("arrival_time", "click_arrival_time").withColumnRenamed("processing_time", "click_processing_time")
  .writeStream
  .format('delta')
  .queryName("clicks")
  .option('checkpointLocation', "/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/cp")
  .start("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/data"))

# COMMAND ----------

imps = (
      Stream.fromPath("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/bronze/data")
        .primaryKeys('adId')
    )

clicks = (
      Stream.fromPath("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/bronze/data")
      .primaryKeys('adId')
    )

j = (
    imps.join(clicks)
    .onKeys('adId')
    .writeToPath("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks_for_impressions/data")
    .option("checkpointLocation", "/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks_for_impressions/cp")
    .queryName('silver_clicks_for_impressions')
    .start()
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks_for_impressions/data`
