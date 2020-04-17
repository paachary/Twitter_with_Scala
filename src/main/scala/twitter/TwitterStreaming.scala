package twitter

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.{Logger, LoggerFactory}
import twitter4j.Status

/**
 *  The TwitterStreaming class provides an example of generating DStream from a twitter stream object.
 *  Further, this class publishes the twitter stream to a Kafka topic.
 */
class TwitterStreaming {

  /** Init method
   * Responsible for
   * 1. Initializing the sparkSession and sparkConfig
   * 2. Setting up the Twitter stream
   * based on a filter and twitter developer account credentials.
   * 3. Setting up a Spark DStream to create RDDs for tweets' stream.
   * 4. Initializing a Kafka Producer with necessary configurations
   * to publish the tweet events to a Kafka Topic.
   *
   * This method invokes a helper object (ConnectionObject) which initializes the twitter stream,
   * DStream and Kafka Producer.
   *
   */
  def init(): Unit = {

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
              case exception: Exception =>
               innerLogger.info("exception " + exception)
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
  twitterStreaming.init()
}

