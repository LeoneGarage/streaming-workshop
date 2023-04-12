# Databricks notebook source
from pyspark.sql.functions import rand

# spark.conf.set("spark.sql.shuffle.partitions", "1")

impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "1000").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)

clicks = (
  spark
  .readStream.format("rate").option("rowsPerSecond", "1000").load()
  .where((rand() * 100).cast("integer") < 10)      # 10 out of every 100 impressions result in a click
  .selectExpr("(value - 100) AS adId ", "timestamp AS clickTime")      # -100 so that a click with same id as impression is generated later (i.e. delayed data).
  .where("adId > 0")
)    

# COMMAND ----------

impressions.writeStream.format("delta").option("checkpointLocation", "/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/cp").start("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/data")

# COMMAND ----------

clicks.writeStream.format("delta").option("checkpointLocation", "/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/cp").start("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/data")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/data`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/data` a
# MAGIC JOIN delta.`/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/data` b ON a.adId = b.adId
