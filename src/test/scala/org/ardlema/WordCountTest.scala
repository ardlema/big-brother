package org.ardlema

import java.nio.file.Files
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.scalatest._
import org.scalatest.concurrent.Eventually

class WordCountTest
  extends FlatSpec
  with GivenWhenThen
  with Eventually
  with Matchers
  with BeforeAndAfter {

  val windowDuration = Seconds(4)
  val slideDuration = Seconds(2)
  val master = "local[2]"
  val appName = "example-spark-streaming"
  val batchDuration = Seconds(1)
  val checkpointDir = Files.createTempDirectory(appName).toString
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, batchDuration)
  ssc.checkpoint(checkpointDir)

  after {
    if (sc != null) sc.stop()
    if (ssc != null) ssc.stop(stopSparkContext = false, stopGracefully = false)
  }

  behavior of "WordCounter"

  "Sample set" should "be counted" in {
     val clock = new ClockWrapper(ssc)

     Given("streaming context is initialized")
     val lines = mutable.Queue[RDD[String]]()

     var results = ListBuffer.empty[Array[WordCount]]

     WordCount.count(ssc.queueStream(lines), windowDuration, slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
       results += wordsCount.collect()
     }

     ssc.start()

     When("first set of words queued")
     lines += sc.makeRDD(Seq("a", "b"))

     Then("words counted after first slide")
     clock.advance(slideDuration.milliseconds)
     eventually(timeout(4.seconds)) {
       results.last should equal(Array(
         WordCount("a", 1),
         WordCount("b", 1)))
     }

     When("second set of words queued")
     lines += sc.makeRDD(Seq("b", "c"))

     Then("words counted after second slide")
     clock.advance(slideDuration.milliseconds)
     eventually(timeout(4.seconds)) {
       results.last should equal(Array(
         WordCount("a", 1),
         WordCount("b", 2),
         WordCount("c", 1)))
     }

     When("nothing more queued")

     Then("word counted after third slide")
     clock.advance(slideDuration.milliseconds)
     eventually(timeout(4.seconds)) {
       results.last should equal(Array(
         WordCount("a", 0),
         WordCount("b", 1),
         WordCount("c", 1)))
     }

     When("nothing more queued")

     Then("word counted after fourth slide")
     clock.advance(slideDuration.milliseconds)
     eventually(timeout(4.seconds)) {
       results.last should equal(Array(
         WordCount("a", 0),
         WordCount("b", 0),
         WordCount("c", 0)))
     }
   }
}
