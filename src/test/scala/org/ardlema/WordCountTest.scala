package org.ardlema

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class WordCountTest
  extends FlatSpec
  with StreamingContextHandler
  with GivenWhenThen {

  val windowDuration = Seconds(4)
  val slideDuration = Seconds(2)

  behavior of "WordCounter"

  "Sample set" should "be counted" in {
   withStreamingContext { ssc =>
     Given("streaming context is initialized")
     val lines = mutable.Queue[RDD[String]]()

     var results = ListBuffer.empty[Array[WordCount]]

     WordCount.count(ssc.queueStream(lines), windowDuration, slideDuration) { (wordsCount: RDD[WordCount], time: Time) =>
       results += wordsCount.collect()
     }


   }
  }
}
