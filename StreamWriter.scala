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

// MAGIC %python
// MAGIC 
// MAGIC def gen_dataset():
// MAGIC   def gen_datetime(min_year=2018, max_year=datetime.now().year):
// MAGIC       # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
// MAGIC       start = datetime(min_year, 1, 1, 00, 00, 00)
// MAGIC       years = max_year - min_year + 1
// MAGIC       end = start + timedelta(days=365 * years)
// MAGIC       return start + (end - start) * random.random()
// MAGIC 
// MAGIC   ops = ["I", "U"]
// MAGIC 
// MAGIC   step = 1000000
// MAGIC   outDf = None
// MAGIC   for i in range(0, 1):
// MAGIC     df = sc.parallelize([Row(table="Table{x}".format(x=i+1), op=random.choice(ops), ts=datetime.now(), id=random.randint(1, 100000000), name="Test{x}".format(x=x), value="Value{x}".format(x=x)) for x in range(i * step, (i+1) * step)]).toDF()
// MAGIC     if outDf == None:
// MAGIC       outDf = df
// MAGIC     else:
// MAGIC       outDf = outDf.union(df)
// MAGIC   outDf.repartition(10).write.format("parquet").partitionBy("table").mode("append").save(parquetLocation)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC letters = string.ascii_lowercase
// MAGIC 
// MAGIC def get_random_string(length):
// MAGIC     return ''.join(random.choice(letters) for i in range(length))
// MAGIC 
// MAGIC def gen_table(tableName):
// MAGIC   ops = ["I", "U"]
// MAGIC 
// MAGIC   step = 1000000
// MAGIC   outDf = None
// MAGIC   for i in range(0, 1):
// MAGIC     if tableName == "Table1":
// MAGIC       df = sc.parallelize([Row(table=tableName,
// MAGIC                                op=random.choice(ops), ts=datetime.now(),
// MAGIC                                id=random.randint(1, 100000000),
// MAGIC                                name="Test{x}".format(x=x),
// MAGIC                                value="Value{x}".format(x=x),
// MAGIC                                amount=random.randint(1, 10000)) for x in range(i * step, (i+1) * step)]).toDF()
// MAGIC     else:
// MAGIC       df = sc.parallelize([Row(table=tableName,
// MAGIC                                op=random.choice(ops), ts=datetime.now(),
// MAGIC                                id=random.randint(1, 100000000),
// MAGIC                                attribute1=get_random_string(20),
// MAGIC                                attribute2=get_random_string(40),
// MAGIC                                attribute3=get_random_string(10)) for x in range(i * step, (i+1) * step)]).toDF()
// MAGIC     if outDf == None:
// MAGIC       outDf = df
// MAGIC     else:
// MAGIC       outDf = outDf.union(df)
// MAGIC   outDf.repartition(10).write.format("parquet").mode("append").save("{}/{}".format(parquetLocation, tableName))

// COMMAND ----------

// MAGIC %python
// MAGIC for i in range(0, 50):
// MAGIC   gen_table("Table2")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC df = spark.read.format("parquet").load("{}/{}".format(parquetLocation, "Table1"))
// MAGIC df.count()

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

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.Trigger

val tableNames = (1 to 1).map(n => s"Table$n")

val awsAccessKeyId = dbutils.secrets.get("leone", "key")
val awsSecretKey = dbutils.secrets.get("leone", "secret")
val checkPointDir = "s3a://databricks-leone/cp"

for(index <- (0 until tableNames.length).par) {
  val tableName = tableNames(index)
  val schema = spark.read.format("parquet").load(parquetLocation + s"/$tableName").schema
  val cols = schema.fields.map(c => col(c.name))
  val testMsg = spark.readStream
    .schema(schema)
    .format("parquet")
    .option("maxFilesPerTrigger", 16)
    .load(parquetLocation + s"/$tableName")
    .repartition(16)

  val TOPIC_DML = s"leone-cdc".toLowerCase()
  testMsg
    .select(col("id").cast("string").as("partitionKey"), to_json(struct(cols:_*)).cast("binary").as("data")).as[(String, Array[Byte])]
    .writeStream
    .outputMode("append")
    .queryName(tableName)
    .foreach(KinesisSink(TOPIC_DML, "us-west-2", awsAccessKeyId, awsSecretKey))
    .option("checkpointLocation", checkPointDir+s"/$tableName")
    .start()
}
