package org.ardlema

import java.nio.file.Files
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import _root_.kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.kafka.KafkaProducer
import org.scalatest._
import org.scalatest.concurrent.Eventually

class DirectKakfaWordCountTest
  extends FlatSpec
  with GivenWhenThen
  with Eventually
  with Matchers
  with BeforeAndAfter
  with KafkaProducer {

  val appName = "kafka-word-count-app"
  val windowDuration = Seconds(4)
  val slideDuration = Seconds(2)
  val batchDuration = Seconds(1)
  val sparkContext = createSparkContext
  val sparkStreamingContext = createSparkStreamingContext
  val checkpointDir = Files.createTempDirectory(appName).toString
  sparkStreamingContext.checkpoint(checkpointDir)

  behavior of "WordCounter"
  "Sample set of messages" should "be counted" in {
    withKafkaProducer { kafkaProducer =>
      val clock = new ClockWrapper(sparkStreamingContext)
      val topic = "testTopic"
      //We need to put a dummy message in the topic to avoid the "Couldn't find leader offsets for Set()" error
      val dummyMessage = new KeyedMessage[String, String](topic, "dummyMessage")
      kafkaProducer.send(dummyMessage)

      Given("streaming context is initialized and we have at least one message in the kafka topic")
      val topics = Set(topic)
      val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        sparkStreamingContext, kafkaParams, topics)
      //TODO: Try to get rid of this mutable variable!!
      var results = ListBuffer.empty[Array[DirectKakfaWordCount]]
      DirectKakfaWordCount.countKafkaMessages(messages, windowDuration, slideDuration) {
        (wordsCount: RDD[DirectKakfaWordCount], time: Time) =>
          results += wordsCount.collect()
      }
      sparkStreamingContext.start()

      When("first set of words sent to the broker")
      val messageAmparo = new KeyedMessage[String, String](topic, "amparo")
      val messageManuela = new KeyedMessage[String, String](topic, "manuela")
      kafkaProducer.send(messageAmparo)
      kafkaProducer.send(messageManuela)

      Then("words counted after first slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs(
          Array(
            DirectKakfaWordCount("amparo", 1),
            DirectKakfaWordCount("manuela", 1)))
      }

      When("second set of words sent to the broker")
      val messageAntonia = new KeyedMessage[String, String](topic, "antonia")
      kafkaProducer.send(messageManuela)
      kafkaProducer.send(messageAntonia)

      Then("words counted after second slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs(
          Array(
            DirectKakfaWordCount("amparo", 1),
            DirectKakfaWordCount("manuela", 2),
            DirectKakfaWordCount("antonia", 1)))
      }

      When("nothing more sent to the broker")

      Then("word counted after third slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs(
          Array(
            DirectKakfaWordCount("amparo", 0),
            DirectKakfaWordCount("manuela", 1),
            DirectKakfaWordCount("antonia", 1)))
      }

      When("nothing more sent to the broker")

      Then("word counted after fourth slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs(
          Array(
            DirectKakfaWordCount("amparo", 0),
            DirectKakfaWordCount("manuela", 0),
            DirectKakfaWordCount("antonia", 0)))
      }
    }
  }

  def createSparkContext = {
    val master = "local[2]"
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    new SparkContext(conf)
  }

  def createSparkStreamingContext = new StreamingContext(sparkContext, batchDuration)

  after {
    if (sparkContext != null) sparkContext.stop()
    if (sparkStreamingContext != null) sparkStreamingContext.stop(stopSparkContext = false, stopGracefully = false)
  }
}
