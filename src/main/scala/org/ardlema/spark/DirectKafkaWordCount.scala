package org.ardlema.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream

case class DirectKakfaWordCount(word: String, count: Int)

object DirectKakfaWordCount {
  type WordHandler = (RDD[DirectKakfaWordCount], Time) => Unit

  def countKafkaMessages(messages: InputDStream[(String, String)],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = {
    val wordCounts = messages.map(x =>
      (x._2, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
        case (word: String, count: Int) => DirectKakfaWordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[DirectKakfaWordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }
}
