package org.ardlema.spark

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Duration, Time}
import org.ardlema.parser.{Message, ParseError, MessageParser}

import scalaz.{\/, \/-}

case class Dwell(userId: Long, geometry: Geometry)

class DwellDetector {
  type DwellHandler = (RDD[Dwell], Time) => Unit

  def detectDwells(events: InputDStream[(String, String)],
                         windowDuration: Duration,
                         slideDuration: Duration)
                        (handler: DwellHandler): Unit = {
    //val seqOp = (v1: Seq[Geometry], v2: Geometry) => (v1 :+ v2)
    //val combOp = (v1: Seq[Geometry], v2: Seq[Geometry]) => (v1 ++ v2)
    val geometriesPerUser = events
      .filter(input => properMessage(input._2))
      .map(e => {
          val message = MessageParser.parse(e._2).toOption.get
          (message.userId, message.geometry)
        }).groupByKeyAndWindow(windowDuration, slideDuration)

    //TODO: Find out geometries that intersect to each other
    /*dwellDetected.foreachRDD((rdd: RDD[Dwell], time: Time) => {
      handler(rdd.sortBy(_.userId), time)
    })*/
  }
  
  def properMessage(message: String): Boolean = if (MessageParser.parse(message).isRight) true else false
}
