package org.ardlema

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{InputDStream, DStream}

case class WordCount(word: String, count: Int)

object WordCount {
  type WordHandler = (RDD[WordCount], Time) => Unit

  def countKafkaMessages(messages: InputDStream[(String, String)],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = {
    val wordCounts = messages.map(x =>
      (x._2, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
        case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }
}
