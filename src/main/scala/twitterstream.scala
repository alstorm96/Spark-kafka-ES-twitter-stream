
import java.sql.Timestamp
import org.apache.spark.sql.expressions._
import spark.implicits._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import sun.util.logging.PlatformLogger.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}


object twitterstream {
  def main(args: Array[String]): Unit =
  {

    val spark = SparkSession.
      builder.appName(name = "Get twitter data").
      master("local[*]").
      getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    

    

    val line = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
      option("subscribe", "trump").
      load().
      selectExpr("CAST(value AS STRING)", "timestamp").
      as[(String, Timestamp)]

    val words = line.select(split(line("value")," ").alias("words"))

    val hashtags = words.select(explode(words("words")).alias("word")).where(col("word").startsWith("#"))

    val hashtagcount = hashtags.groupBy(window($"timestamp", "60 seconds"), $"word")
      .agg(count("word").alias("hashtag_count")).orderBy(desc("hashtag_count"))

       hashtagcount.writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/home/tmp/ELKcheck")
      .option("es.port", "9200")
      .option("es.nodes", "localhost")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start("twittertags/doc") //indexname/type
      .awaitTermination()

    spark.streams.awaitAnyTermination()

  }


}
