package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

class ClockWrapper(ssc: StreamingContext) {

  val manualClock = ssc.scheduler.clock.asInstanceOf[ManualClock]

  def getTimeMillis: Long = manualClock.getTimeMillis()

  def setTime(timeToSet: Long) = manualClock.setTime(timeToSet)

  def advance(timeToAdd: Long) = manualClock.advance(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock.waitTillTime(targetTime)
}
