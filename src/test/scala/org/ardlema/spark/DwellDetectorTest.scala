package org.ardlema.spark

import java.nio.file.Files

import _root_.kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.ardlema.geometry.GeomUtils
import org.ardlema.parser.MessageParser
import org.kafka.KafkaProducer
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class DwellDetectorTest
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
  val dwellsThreshold = 1
  val sparkContext = createSparkContext
  val sparkStreamingContext = createSparkStreamingContext
  val checkpointDir = Files.createTempDirectory(appName).toString
  sparkStreamingContext.checkpoint(checkpointDir)
  val brokerPort = 9093

  behavior of "DwellDetector"
  "A user daily activity" should "produced dwells" in {
    withKafkaProducer(zkPort = 2182, brokerPort = brokerPort, kafkaProducer => {
      val clock = new ClockWrapper(sparkStreamingContext)
      val topic = "dwellDetectorTopic"
      //We need to put a dummy message in the topic to avoid the "Couldn't find leader offsets for Set()" error
      val dummyMessage = new KeyedMessage[String, String](topic, "dummyMessage")
      kafkaProducer.send(dummyMessage)

      Given("streaming context is initialized and we have at least one message in the kafka topic")
      val topics = Set(topic)
      val kafkaParams = Map[String, String]("metadata.broker.list" -> s"localhost:$brokerPort")
      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        sparkStreamingContext, kafkaParams, topics)
      //TODO: Try to get rid of this mutable variable!!
      var results = ListBuffer.empty[Array[Dwell]]
      DwellDetector.detectDwells(messages, windowDuration, slideDuration, dwellsThreshold) {
        (dwell: RDD[Dwell], time: Time) =>
          results += dwell.collect()
      }
      sparkStreamingContext.start()

      When("first set of events sent to the broker")
      val colladoVillalbaLocation1 = "1234|POLYGON(" +
        "(-3.9968132972717285 40.63518634434282," +
        "-3.9978432655334473 40.63451871274505," +
        "-3.9962339401245117 40.63422560408105," +
        "-3.9968132972717285 40.63518634434282))"
      val colladoVillalbaLocation2 = "1234|POLYGON(" +
        "(-3.9963412284851074 40.63498763304449," +
        "-3.997328281402588 40.63396175429608," +
        "-3.9955687522888184 40.63402688992333," +
        "-3.9963412284851074 40.63498763304449))"
      val colladoVillalbaGeom1 = MessageParser.parse(colladoVillalbaLocation1).toOption.get.geometry
      val colladoVillalbaGeom2 = MessageParser.parse(colladoVillalbaLocation2).toOption.get.geometry
      val houseDwellGeom = colladoVillalbaGeom1.intersection(colladoVillalbaGeom2)
      val eventHouse1 = new KeyedMessage[String, String](topic, colladoVillalbaLocation1)
      val eventHouse2 = new KeyedMessage[String, String](topic, colladoVillalbaLocation2)
      kafkaProducer.send(eventHouse1)
      kafkaProducer.send(eventHouse2)

      Then("Dwells detected after first slide")
      clock.advance(slideDuration.milliseconds)
      eventually(timeout(4.seconds)) {
        results.last should contain theSameElementsAs (Array(Dwell(1234, houseDwellGeom)))
      }
    })
    }



      /*When("second set of words sent to the broker")
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
    }*/

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
