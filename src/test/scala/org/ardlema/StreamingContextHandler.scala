package org.ardlema

import java.nio.file.Files

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{StreamingContext, Seconds}

trait StreamingContextHandler {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  def withStreamingAndSparkContext(func:(StreamingContext, SparkContext) => Any): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, batchDuration)
    ssc.checkpoint(checkpointDir)
    func(ssc, sc)
    if (sc != null) sc.stop()
    if (ssc != null) ssc.stop(stopSparkContext = false, stopGracefully = false)
  }
}
