// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
  }
    //TODO: Find out geometries that intersect to each other
    /*dwellDetected.foreachRDD((rdd: RDD[Dwell], time: Time) => {
      handler(rdd.sortBy(_.userId), time)
    })*/
  def properMessage(message: String): Boolean = if (MessageParser.parse(message).isRight) true else false
}
