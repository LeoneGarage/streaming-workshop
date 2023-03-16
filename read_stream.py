# Databricks notebook source
from pyspark.sql.functions import col, from_json, sum, count, current_timestamp, window
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

def cleanup_checkpoint():
  dbutils.fs.rm("s3a://databricks-leone/streaming-workshop/cp", True)

def cleanup_data():
  dbutils.fs.rm("s3a://databricks-leone/streaming-workshop/data", True)

def cleanup():
  cleanup_checkpoint()
  cleanup_data()

def dataframe():
  return spark.read.format('delta'). load("s3a://databricks-leone/streaming-workshop/data")

def display_output():
  display(dataframe())
  
def display_count():
  print(dataframe().count())

# COMMAND ----------

schema = StructType.fromDDL("amount LONG, id LONG, name STRING, op STRING, table STRING, ts TIMESTAMP, value STRING")
cols = [f.name for f in schema.fields]

def readStream():
  spark.conf.set("spark.sql.shuffle.partitions", 32)
  awsAccessKeyId = dbutils.secrets.get("leone", "key")
  awsSecretKey = dbutils.secrets.get("leone", "secret")
  kinesisStreamName = "leone-cdc"
  kinesisRegion = "us-west-2"
  kinesisStreamDf = (
                      spark.readStream
                      .format("kinesis")
                      .option("streamName", kinesisStreamName)
                      .option("region", kinesisRegion)
                      .option("initialPosition", "TRIM_HORIZON")
                      .option("awsAccessKey", awsAccessKeyId)
                      .option("awsSecretKey", awsSecretKey)
                      .load()
                    )
  return kinesisStreamDf

# COMMAND ----------

# DBTITLE 1,Simple read from Kinesis
kinesisStreamDf = readStream()
display(kinesisStreamDf)

# COMMAND ----------

# DBTITLE 1,Extracted read from Kinesis
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in cols] + [col("arrival_time"), current_timestamp().alias("processing_time")])
display(dataStreamDf)

# COMMAND ----------

# DBTITLE 1,Checkpoint
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("kinesis-reader")
                .format("delta")
                .option("checkpointLocation", "s3a://databricks-leone/streaming-workshop/cp")
                .start("s3a://databricks-leone/streaming-workshop/data")
              )

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls s3a://databricks-leone/streaming-workshop/cp/commits

# COMMAND ----------

cleanup()

# COMMAND ----------

# DBTITLE 1,Simple aggregation
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
dataStreamDf = dataStreamDf.select(sum("amount").alias("sum"))
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("kinesis-reader")
                .format("delta")
                .outputMode("complete")
                .option("checkpointLocation", "s3a://databricks-leone/streaming-workshop/cp")
                .start("s3a://databricks-leone/streaming-workshop/data")
              )

# COMMAND ----------

cleanup()

# COMMAND ----------

# DBTITLE 1,GroupBy Aggregation and runaway state
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
dataStreamDf = dataStreamDf.groupBy("id").agg(sum("amount").alias("sum"))
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("kinesis-reader")
                .format("delta")
                .outputMode("complete")
                .option("checkpointLocation", "s3a://databricks-leone/streaming-workshop/cp")
                .start("s3a://databricks-leone/streaming-workshop/data")
              )

# COMMAND ----------

cleanup()

# COMMAND ----------

# DBTITLE 1,GroupBy with Watermark
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
dataStreamDf = dataStreamDf.withWatermark("ts", "20 seconds")
dataStreamDf = dataStreamDf.groupBy(window("ts", "10 seconds"),(col("id") % 2000).alias("group")).agg(sum("amount").alias("sum"))
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("kinesis-reader")
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", "s3a://databricks-leone/streaming-workshop/cp")
                .start("s3a://databricks-leone/streaming-workshop/data")
              )

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls s3a://databricks-leone/streaming-workshop/cp/state/0

# COMMAND ----------

# DBTITLE 1,Testing Output Sinks
dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
dataStreamDf = dataStreamDf.withWatermark("ts", "20 seconds")
dataStreamDf = dataStreamDf.groupBy(window("ts", "10 seconds", "5 seconds"),(col("id") % 2000).alias("group")).agg(sum("amount").alias("sum"))
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("memory")
                .format("memory")
                .outputMode("update")
                .start()
              )

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM memory

# COMMAND ----------

dataStreamDf = readStream().select(from_json(col("data").cast("string"), schema).alias("data")).select([col(f"data.{c}").alias(c) for c in cols])
dataStreamDf = dataStreamDf.withWatermark("ts", "20 seconds")
dataStreamDf = dataStreamDf.groupBy(window("ts", "10 seconds", "5 seconds"),(col("id") % 2000).alias("group")).agg(sum("amount").alias("sum"))
queryStream = (
                dataStreamDf
                .writeStream
                .queryName("console")
                .format("console")
                .outputMode("update")
                .start()
              )

# COMMAND ----------

# DBTITLE 1,Testing Sources
rateStreamDf = (
                      spark.readStream
                      .format("rate")
                      .option("rowsPerSecond", 100)
                      .option("rampUpTime", "10s")
                      .option("numPartitions", 10)
                      .load()
                    )
display(rateStreamDf)

# COMMAND ----------

rateStreamDf = (
                      spark.readStream
                      .format("rate-micro-batch")
                      .option("numPartitions", 10)
                      .option("rowsPerBatch", 100)
                      .option("startTimestamp", 100)
                      .option("advanceMillisPerBatch", 1000)
                      .load()
                    )
display(rateStreamDf)
