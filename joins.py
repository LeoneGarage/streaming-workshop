# Databricks notebook source
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

impDf = readStream("leone-impressions").select(col("data").cast("string"))
# display(impDf)

# COMMAND ----------

clicksDf = readStream("leone-clicks").select(col("data").cast("string"))
# display(clicksDf)

# COMMAND ----------

impStreamDf = readStream("leone-impressions").select(from_json(col("data").cast("string"), "adId INT, impressionTime TIMESTAMP").alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in ["adId", "impressionTime"]] + [col("arrival_time"), current_timestamp().alias("processing_time")])
# display(impStreamDf)

# COMMAND ----------

clickStreamDf = readStream("leone-clicks").select(from_json(col("data").cast("string"), "adId INT, clickTime TIMESTAMP").alias("data"), col("approximateArrivalTimestamp").alias("arrival_time")).select([col(f"data.{c}").alias(c) for c in ["adId", "clickTime"]] + [col("arrival_time"), current_timestamp().alias("processing_time")])
# display(clickStreamDf)

# COMMAND ----------

display(impStreamDf.join(clickStreamDf, "adId"))

# COMMAND ----------

impStreamDf.join(clickStreamDf, "adId").writeStream.format("memory").queryName("join").start()

# COMMAND ----------

# Define watermarks
impressionsWithWatermark = impStreamDf \
  .selectExpr("adId AS impressionAdId", "impressionTime") \
  .withWatermark("impressionTime", "10 seconds ")
clicksWithWatermark = clickStreamDf \
  .selectExpr("adId AS clickAdId", "clickTime") \
  .withWatermark("clickTime", "20 seconds")        # max 20 seconds late


# Inner join with time range conditions
(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes 
      """
    )
  ).writeStream.format("memory").queryName("joinWithWatermark").start()
)

# COMMAND ----------

# Define watermarks
impressionsWithWatermark = impStreamDf \
  .selectExpr("adId AS impressionAdId", "impressionTime") \
  .withWatermark("impressionTime", "10 seconds ")
clicksWithWatermark = clickStreamDf \
  .selectExpr("adId AS clickAdId", "clickTime") \
  .withWatermark("clickTime", "20 seconds")        # max 20 seconds late


# Inner join with time range conditions
(
  impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes
      """
    ),
    "left"
  ).writeStream.format("memory").queryName("leftJoinWithWatermark").start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM leftJoinWithWatermark

# COMMAND ----------

staticClicks = spark.read.format("delta").load("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/data") \
  .selectExpr("adId AS clickAdId", "clickTime")
(
  impressionsWithWatermark.join(
    staticClicks,
    expr(""" 
      clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes
      """
    )
  ).writeStream.format("memory").queryName("stream_static_join").start()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM stream_static_join

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import java.sql._
# MAGIC 
# MAGIC case class Impression(adId: Int, impressionTime: Timestamp)
# MAGIC case class Click(adId: Int, clickTime: Timestamp)
# MAGIC case class Unioned(impression: Option[Impression], click: Option[Click], eventTime: Timestamp, adId: Int)
# MAGIC case class JoinedState(impression: Impression, click: Click)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.types._
# MAGIC 
# MAGIC def readStream(stream_name: String) = {
# MAGIC   spark.conf.set("spark.sql.shuffle.partitions", 32)
# MAGIC   val awsAccessKeyId = dbutils.secrets.get("leone", "key")
# MAGIC   val awsSecretKey = dbutils.secrets.get("leone", "secret")
# MAGIC   val kinesisStreamName = stream_name
# MAGIC   val kinesisRegion = "us-west-2"
# MAGIC   val kinesisStreamDf = (
# MAGIC                       spark.readStream
# MAGIC                       .format("kinesis")
# MAGIC                       .option("streamName", kinesisStreamName)
# MAGIC                       .option("region", kinesisRegion)
# MAGIC                       .option("initialPosition", "TRIM_HORIZON")
# MAGIC                       .option("maxRecordsPerFetch", "100")
# MAGIC                       .option("awsAccessKey", awsAccessKeyId)
# MAGIC                       .option("awsSecretKey", awsSecretKey)
# MAGIC                       .load()
# MAGIC                     )
# MAGIC   kinesisStreamDf
# MAGIC }
# MAGIC 
# MAGIC val impressions = readStream("leone-impressions").select(from_json(col("data").cast("string"), new StructType().add("adId", IntegerType).add("impressionTime", TimestampType)).alias("data")).select(col("data.*")).as[Impression]
# MAGIC                .map(c => Unioned(Some(c), None, c.impressionTime, c.adId))
# MAGIC val clicks = readStream("leone-clicks").select(from_json(col("data").cast("string"), new StructType().add("adId", IntegerType).add("clickTime", TimestampType)).alias("data")).select(col("data.*")).as[Click]
# MAGIC                .map(s => Unioned(None, Some(s), s.clickTime, s.adId))
# MAGIC 
# MAGIC val unioned = impressions.union(clicks)

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
# MAGIC 
# MAGIC sc.setLocalProperty("spark.scheduler.pool", "flatMapGroupsWithState")
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", 10)
# MAGIC 
# MAGIC val impClicksDf = unioned.withWatermark("eventTime", "20 seconds").groupByKey(_.adId)
# MAGIC .flatMapGroupsWithState(OutputMode.Append, GroupStateTimeout.EventTimeTimeout) {
# MAGIC   (key: Int, rows: Iterator[Unioned], state: GroupState[JoinedState]) => {
# MAGIC     if( state.hasTimedOut ) { // If we timed out remove the state
# MAGIC       // val joinState = if( state.exists ) Some(state.get) else None
# MAGIC       state.remove()
# MAGIC       // joinState.filter(u => u.impression != null && u.click != null).fold(Iterator[(Int, Timestamp, Timestamp)]())(u => Iterator((u.impression.adId, u.impression.impressionTime, u.click.clickTime)))
# MAGIC       Iterator()
# MAGIC     }
# MAGIC     else {
# MAGIC       // Here we are combining Impression rows and Click rows into a single state instance
# MAGIC       val joinState = if( state.exists ) Some(state.get) else None
# MAGIC       val updated = rows.foldLeft(joinState){
# MAGIC         (aggState, row) => {
# MAGIC           row.impression.map(i => {
# MAGIC             aggState.fold(JoinedState(i, null))(js => js.copy(impression = i))
# MAGIC           })
# MAGIC           .orElse {
# MAGIC             row.click.map(c => {
# MAGIC               aggState.fold(JoinedState(null, c))(js => js.copy(click = c))
# MAGIC             })
# MAGIC           }
# MAGIC         }
# MAGIC       }
# MAGIC       // Reset timeout
# MAGIC       state.setTimeoutTimestamp(state.getCurrentWatermarkMs, "20 seconds")
# MAGIC       // Update joined state at this point in time
# MAGIC       updated.foreach(u => state.update(u))
# MAGIC       // If we have both sides of the join set to something emit that row with schema call_id, name, comment, eventTime, session_count
# MAGIC       // session_count is the number of sessions seen at this point in time
# MAGIC       updated.filter(u => u.impression != null && u.click != null).fold(Iterator[(Int, Timestamp, Timestamp)]())(u => Iterator((u.impression.adId, u.impression.impressionTime, u.click.clickTime)))
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC .toDF("adId", "impressionTime", "clickTime")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC impClicksDf.writeStream.format("memory").queryName("flatMapGroupsWithState").start()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM flatMapGroupsWithState

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM ss_demo.impressions_with_clicks
