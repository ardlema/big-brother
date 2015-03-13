package org.ardlema

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

case class WordCount(word: String, count: Int)

object WordCount {
  type WordHandler = (RDD[WordCount], Time) => Unit

  def count(lines: RDD[String]): RDD[WordCount] = count(lines, Set())

  def count(lines: RDD[String], stopWords: Set[String]): RDD[WordCount] = {
    val words = prepareWords(lines, stopWords)

    val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _).map {
      case (word: String, count: Int) => WordCount(word, count)
    }

    val sortedWordCounts = wordCounts.sortBy(_.word)

    sortedWordCounts
  }

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

  private def prepareWords(lines: RDD[String], stopWords: Set[String]): RDD[String] = {
    lines.flatMap(_.split("\\s"))
      .map(_.stripSuffix(",").stripSuffix(".").toLowerCase)
      .filter(!stopWords.contains(_)).filter(!_.isEmpty)
  }
}
