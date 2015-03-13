package org.ardlema

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}

trait StreamingContextHandler {

  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  def withStreamingContext(func:(StreamingContext) => Any): Unit = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val ssc: StreamingContext = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)
    val sc = ssc.sparkContext
    func(ssc)
    if (ssc != null) ssc.stop()
  }
}
