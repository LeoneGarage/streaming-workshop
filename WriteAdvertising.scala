// Databricks notebook source
// MAGIC %python
// MAGIC import random, string
// MAGIC from pyspark.sql import Row
// MAGIC from datetime import datetime, timedelta

// COMMAND ----------

//fs.s3n.awsAccessKeyId {{secrets/leone/key}}
//fs.s3n.awsSecretAccessKey {{secrets/leone/secret}}

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val parquetLocation = "s3a://databricks-leone/test_stream"

// COMMAND ----------

// MAGIC %python
// MAGIC parquetLocation = "s3a://databricks-leone/test_stream"

// COMMAND ----------

// %python

// def gen_dataset():
//   def gen_datetime(min_year=2018, max_year=datetime.now().year):
//       # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
//       start = datetime(min_year, 1, 1, 00, 00, 00)
//       years = max_year - min_year + 1
//       end = start + timedelta(days=365 * years)
//       return start + (end - start) * random.random()

//   ops = ["I", "U"]

//   step = 1000000
//   outDf = None
//   for i in range(0, 1):
//     df = sc.parallelize([Row(table="Table{x}".format(x=i+1), op=random.choice(ops), ts=datetime.now(), id=random.randint(1, 100000000), name="Test{x}".format(x=x), value="Value{x}".format(x=x)) for x in range(i * step, (i+1) * step)]).toDF()
//     if outDf == None:
//       outDf = df
//     else:
//       outDf = outDf.union(df)
//   outDf.repartition(10).write.format("parquet").partitionBy("table").mode("append").save(parquetLocation)

// COMMAND ----------

// %python

// letters = string.ascii_lowercase

// def get_random_string(length):
//     return ''.join(random.choice(letters) for i in range(length))

// def gen_table(tableName):
//   ops = ["I", "U"]

//   step = 1000000
//   outDf = None
//   for i in range(0, 1):
//     if tableName == "Table1":
//       df = sc.parallelize([Row(table=tableName,
//                                op=random.choice(ops), ts=datetime.now(),
//                                id=random.randint(1, 100000000),
//                                name="Test{x}".format(x=x),
//                                value="Value{x}".format(x=x),
//                                amount=random.randint(1, 10000)) for x in range(i * step, (i+1) * step)]).toDF()
//     else:
//       df = sc.parallelize([Row(table=tableName,
//                                op=random.choice(ops), ts=datetime.now(),
//                                id=random.randint(1, 100000000),
//                                attribute1=get_random_string(20),
//                                attribute2=get_random_string(40),
//                                attribute3=get_random_string(10)) for x in range(i * step, (i+1) * step)]).toDF()
//     if outDf == None:
//       outDf = df
//     else:
//       outDf = outDf.union(df)
//   outDf.repartition(10).write.format("parquet").mode("append").save("{}/{}".format(parquetLocation, tableName))

// COMMAND ----------

// %python
// for i in range(0, 50):
//   gen_table("Table2")

// COMMAND ----------

// %python

// df = spark.read.format("parquet").load("{}/{}".format(parquetLocation, "Table1"))
// df.count()

// COMMAND ----------

import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Buffer}

import com.amazonaws.auth._
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis._
import com.amazonaws.services.kinesis.model._

import org.apache.spark.sql.ForeachWriter

/**
 * A simple Sink that writes to the given Amazon Kinesis `stream` in the given `region`. For authentication, users may provide
 * `awsAccessKey` and `awsSecretKey`, or use IAM Roles when launching their cluster.
 *
 * This Sink takes a two column Dataset, with the columns being the `partitionKey`, and the `data` respectively.
 * We will buffer data up to `maxBufferSize` before flushing to Kinesis in order to reduce cost.
 */
class KinesisSink(
    stream: String,
    region: String,
    awsAccessKey: Option[String] = None,
    awsSecretKey: Option[String] = None) extends ForeachWriter[(String, Array[Byte])] { 

  // Configurations
  private val maxBufferSize = 500 * 1024 // 500 KB
  private val maxRetries = 10
  private val retryDelayMS = 100
  
  private var client: AmazonKinesis = _
  private val buffer = new ArrayBuffer[PutRecordsRequestEntry]()
  private var bufferSize: Long = 0L

  override def open(partitionId: Long, version: Long): Boolean = {
    client = createClient
    true
  }

  override def process(value: (String, Array[Byte])): Unit = {
    val (partitionKey, data) = value
    // Maximum of 500 records can be sent with a single `putRecords` request
    if ((data.length + bufferSize > maxBufferSize && buffer.nonEmpty) || buffer.length == 500) {
      flush()
    }
    buffer += new PutRecordsRequestEntry().withPartitionKey(partitionKey).withData(ByteBuffer.wrap(data))
    bufferSize += data.length
  }

  override def close(errorOrNull: Throwable): Unit = {
    if (buffer.nonEmpty) {
      flush()
    }
    client.shutdown()
  }
  
  /** Flush the buffer to Kinesis */
  private def flush(): Unit = {
    var recordsToProcess: Buffer[PutRecordsRequestEntry] = buffer
    var failedExceptions: Buffer[String] = Buffer.empty
    var retryCount = 0
    do {
      val recordRequest = new PutRecordsRequest()
        .withStreamName(stream)
        .withRecords(recordsToProcess: _*)

      val result = client.putRecords(recordRequest)
      if( result.getFailedRecordCount() > 0 ) {
        val failedResults  = result.getRecords().zip(recordsToProcess)
          .filter {
            case (rs, rq) => rs.getErrorCode() != null
           }
          .map {
            case (rs, rq) => (rs.getErrorMessage(), rq)
          }
        recordsToProcess = failedResults.map {
          case (_, rq) => rq
        }
        failedExceptions = failedResults.map {
          case (errorMsg, _) => errorMsg
        }
      }
      else {
        recordsToProcess = Buffer.empty
      }
      if( recordsToProcess.nonEmpty ) {
        val delayMS = scala.math.pow(2, retryCount).toLong * retryDelayMS
        println(s"After ${retryCount+1} retries, still ${recordsToProcess.length} records to process with ${failedExceptions.headOption.getOrElse("")}\nSleeping for $delayMS ms")
        Thread.sleep(delayMS)
        retryCount += 1
      }
    } while(recordsToProcess.nonEmpty && retryCount < maxRetries)
    if( recordsToProcess.nonEmpty ) {
      throw new ProvisionedThroughputExceededException(s"After $maxRetries retries, still ${recordsToProcess.length} records to process with ${failedExceptions.headOption.getOrElse("")}")
    }
    buffer.clear()
    bufferSize = 0
  }
  
  /** Create a Kinesis client. */
  private def createClient: AmazonKinesis = {
    val cli = if (awsAccessKey.isEmpty || awsSecretKey.isEmpty) {
      AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .build()
    } else {
      AmazonKinesisClientBuilder.standard()
        .withRegion(region)
        .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey.get, awsSecretKey.get)))
        .build()
    }
    cli
  }
}

object KinesisSink {
  def apply(stream: String,
            region: String,
            awsAccessKey: String,
            awsSecretKey: String): ForeachWriter[(String, Array[Byte])] = new KinesisSink(stream, region, Some(awsAccessKey), Some(awsSecretKey))
  def apply(stream: String,
            region: String): ForeachWriter[(String, Array[Byte])] = new KinesisSink(stream, region)
}

// COMMAND ----------

dbutils.fs.rm("s3a://databricks-leone/advertising/impressions/cp", true)

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.Trigger

val awsAccessKeyId = dbutils.secrets.get("leone", "key")
val awsSecretKey = dbutils.secrets.get("leone", "secret")
val checkPointDir = "s3a://databricks-leone/advertising/impressions/cp"

val schema = spark.read.format("delta").load("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/data").schema
val cols = schema.fields.map(c => col(c.name))
val testMsg = spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 40)
  .load("/Users/leon.eller@databricks.com/streaming_workshop/advertising/impressions/data")
  .repartition(40)

val TOPIC_DML = s"leone-impressions".toLowerCase()
testMsg
  .select(col("adid").cast("string").as("partitionKey"), to_json(struct(cols:_*)).cast("binary").as("data")).as[(String, Array[Byte])]
  .writeStream
  .outputMode("append")
  .queryName("impressions")
  .foreach(KinesisSink(TOPIC_DML, "us-west-2", awsAccessKeyId, awsSecretKey))
  .option("checkpointLocation", checkPointDir)
  .start()

// COMMAND ----------

dbutils.fs.rm("s3a://databricks-leone/advertising/clicks/cp", true)

// COMMAND ----------

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.Trigger

val awsAccessKeyId = dbutils.secrets.get("leone", "key")
val awsSecretKey = dbutils.secrets.get("leone", "secret")
val checkPointDir = "s3a://databricks-leone/advertising/clicks/cp"

val schema = spark.read.format("delta").load("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/data").schema
val cols = schema.fields.map(c => col(c.name))
val testMsg = spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 40)
  .load("/Users/leon.eller@databricks.com/streaming_workshop/advertising/clicks/data")
  .repartition(40)

val TOPIC_DML = s"leone-clicks".toLowerCase()
testMsg
  .select(col("adid").cast("string").as("partitionKey"), to_json(struct(cols:_*)).cast("binary").as("data")).as[(String, Array[Byte])]
  .writeStream
  .outputMode("append")
  .queryName("clicks")
  .foreach(KinesisSink(TOPIC_DML, "us-west-2", awsAccessKeyId, awsSecretKey))
  .option("checkpointLocation", checkPointDir)
  .start()
