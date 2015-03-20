package org.ardlema

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{InputDStream, DStream}

case class WordCount(word: String, count: Int)

object WordCount {
  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(messages: InputDStream[(String, String)],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = countKafka(messages, windowDuration, slideDuration)(handler)

  def count(lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = count(lines, windowDuration, slideDuration, Set())(handler)

  def count(lines: DStream[String],
            windowDuration: Duration,
            slideDuration: Duration,
            stopWords: Set[String])
           (handler: WordHandler): Unit = {
    val words = lines.transform(prepareWords(_, stopWords))

    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

  def countKafka(messages: InputDStream[(String, String)],
            windowDuration: Duration,
            slideDuration: Duration)
           (handler: WordHandler): Unit = {
    val wordCounts = messages.map(x => (x._2, 1)).reduceByKeyAndWindow(_ + _, _ - _, windowDuration, slideDuration).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    wordCounts.foreachRDD((rdd: RDD[WordCount], time: Time) => {
      handler(rdd.sortBy(_.word), time)
    })
  }

  private def prepareWords(lines: RDD[String], stopWords: Set[String]): RDD[String] = {
    lines.flatMap(_.split("\\s"))
      .map(_.stripSuffix(",").stripSuffix(".").toLowerCase)
      .filter(!stopWords.contains(_)).filter(!_.isEmpty)
  }
}
