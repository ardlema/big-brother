package org.ardlema

import java.nio.file.Files

import _root_.kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import kafka.serializer.StringDecoder
import kafka.message.Message
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.kafka.KafkaProducer
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.apache.spark.streaming._
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class DirectKakfaWordCountTest
  extends FlatSpec
  with GivenWhenThen
  with Eventually
  with Matchers
  with BeforeAndAfter
  with KafkaProducer {

  val windowDuration = Seconds(4)
  val slideDuration = Seconds(2)
  val master = "local[2]"
  val appName = "example-spark-streaming-direct-kafka"
  val batchDuration = Seconds(1)
  val checkpointDir = Files.createTempDirectory(appName).toString
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, batchDuration)
  ssc.checkpoint(checkpointDir)
  val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")

  after {
    if (sc != null) sc.stop()
    if (ssc != null) ssc.stop(stopSparkContext = false, stopGracefully = false)
  }

  behavior of "WordCounter"

  "Sample set" should "be counted" in {
    withKafkaProducer { kafkaProducer =>
      val clock = new ClockWrapper(ssc)

      val topic = "testTopic"
      val dummyMessage = new KeyedMessage[String, String](topic, "dummyMessage")
      kafkaProducer.send(dummyMessage)

      Given("streaming context is initialized and we have at least one message in the kafka topic")
      val topics = Set(topic)
      val messages: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topics)

      //TODO: Try to get rid of this mutable variable
      var results = ListBuffer.empty[Array[DirectKakfaWordCount]]

      DirectKakfaWordCount.countKafkaMessages(messages, windowDuration, slideDuration) { (wordsCount: RDD[DirectKakfaWordCount], time: Time) =>
        results += wordsCount.collect()
      }

      ssc.start()

      When("first set of words queued")
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

      When("second set of words queued")
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

      When("nothing more queued")

      Then("word counted after third slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs(
          Array(
            DirectKakfaWordCount("amparo", 0),
            DirectKakfaWordCount("manuela", 1),
            DirectKakfaWordCount("antonia", 1)))
      }

      When("nothing more queued")

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
}
