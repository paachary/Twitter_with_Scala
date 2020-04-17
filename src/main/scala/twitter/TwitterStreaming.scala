package twitter

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.{Logger, LoggerFactory}
import twitter4j.{Place, Status}

/**
 *  The TwitterStreaming class provides an example of generating DStream from a twitter stream object.
 *  Further, this class publishes the twitter stream to a Kafka topic.
 */
class TwitterStreaming {

  def init: Unit = {

    val logger: Logger = LoggerFactory.getLogger(TwitterStreaming.getClass.getName)
    logger.info("Starting the tweet producer")
    import ConnectionObject._

    val appName = "TwitterData"
    val topic: String = "tweet_new_topic"
    val filter : Seq[String] = Seq("narendramodi")

    val spark =
      SparkSession.builder().appName(appName).config("spark.master", "local[*]").getOrCreate()

    // Twitter Data
    val (dStreamTweet: DStream[Status], ssc: StreamingContext) = getTwitterDStream(filter, spark.sparkContext)

    // Stream into Kafka
    dStreamTweet.foreachRDD{ rdd : RDD[Status] =>
        rdd.foreachPartition { partitionOfRecords =>
          // Kafka Producer
          val innerLogger = LoggerFactory.getLogger(TwitterStreaming.getClass.getName+"_dStream")
          val producer: KafkaProducer[String, String] = createKafkaProducer
          partitionOfRecords.foreach { record =>
            // forming the key
            val key = record.getId.toString

            // forming the value which should be concatenation of created_time, user and the text msg
            val value = (record.getCreatedAt.getTime / 1000).toString + "~~" +
              record.getUser.getScreenName + "~~" +
              record.getText
            innerLogger.info("key:" + key + ", value:" + value)
            try {
              // Creating a producer record containing the topic, key and value for kafka producer
              sendProducerRecord(topic, key, value, producer)
            } catch {
              case exception: Exception => {
               innerLogger.info("exception " + exception)
              }
            }
          }
          innerLogger.info("Closing the producer")
          producer.close()
        }
    }
    logger.info("Starting the streaming...")
    ssc.start()
    ssc.awaitTermination()
    logger.info("Shutting down..")
  }
}

/**
 * This is the companion object of the TwitterStreaming class which extends the App Trait.
 * The object invokes the instance of the class via its method: init, which initiates the twitter stream
 * and publishes into a Kafka topic.
 */
object TwitterStreaming extends App {
  val twitterStreaming = new TwitterStreaming
  val logger = LoggerFactory.getLogger(TwitterStreaming.getClass.getName+"_main")
  logger.info("Invoking the main method")
  twitterStreaming.init
}

