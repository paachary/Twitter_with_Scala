package twitter

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.slf4j.{Logger, LoggerFactory}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

/** This is a helper object which
 * 1. sets up the Twitter stream based on a filter
 * and twitter developer account credentials.
 * 2. sets up a Spark DStream to create RDDs for tweets' stream.
 * 3. initializes a Kafka Producer with necessary configurations
 * to publish the tweet events to a Kafka Topic.
 *
 */
object ConnectionObject {

  val logger: Logger = LoggerFactory.getLogger(ConnectionObject.getClass.getName)

  /**
   * function to create a kafka producer based on certain kafka configurations.
   * @return KafkaProducer[String,String]
   */
  def createKafkaProducer : KafkaProducer[String,String] = {
    logger.info("Preparing the Kafka Configuration Properties")
    val props = new Properties()
    val bootStrapServers: String = "localhost:9092"

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "20")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024))

    new KafkaProducer[String, String](props)
  }

  /***
   * function to parse the twitter credentials and populate the string elements.
   * @return a tupple of 4 string elements
   */
  // Parsing the twitter file for getting the authentication string
  def parseTwitterCred: (String, String, String, String ) = {

    import scala.io.Source

    logger.info("Preparing the twitter developer authentication")
    try {
      for (line <- Source.fromFile( "twitter.txt"). getLines) {
        val fields = line.split(" ")
        if (fields.length == 2) {
          System.setProperty("twitter4j.oauth." + fields(0), fields(1))
        }
      }
      (System.getProperty("twitter4j.oauth.consumerKey"),
        System.getProperty("twitter4j.oauth.consumerSecret"),
        System.getProperty("twitter4j.oauth.accessToken"),
        System.getProperty("twitter4j.oauth.accessTokenSecret"))
    } catch {
      case exception: FileNotFoundException =>
        logger.error("Error while opening the file")
        ("n/a","n/a","n/a","n/a")
    }
  }

  /**
   * function returning a DStream of Twitter events data
   * along with sparkContext
   * @param filter : a\n Array of String containing filters
   * @param sparkContext : spark context
   * @return DStream : DStream containing a twitter event
   *         sparkContext
   */
  def getTwitterDStream(filter: Seq[String], sparkContext: SparkContext): (DStream[Status], StreamingContext) = {

    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val (consumerKey,
    consumerSecret,
    accessToken,
    accessTokenSecret) = parseTwitterCred

    val cb = new ConfigurationBuilder

    logger.info("Building the configuration with Twitter Developer authentication")

    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).
      setOAuthConsumerSecret(consumerSecret).
      setOAuthAccessToken(accessToken).
      setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(cb.build)

    logger.info("Creating the twitter stream")
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filter)

    ( tweets.window(Seconds(600)), ssc)
  }

  /**
   * function submitting a new Kafka Record object for a given topic
   * and key/value pair via a Kafka producer
   * @param topic : String containing the topic name
   * @param key: the key for the topic
   * @param value: the value for the topic
   * @param producer: the kafka producer for sending the record to kafka.
   */
  def sendProducerRecord( topic: String, key: String, value: String, producer: KafkaProducer[String, String]) :
  Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, value))
  }
}

