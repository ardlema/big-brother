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
package org.ardlema.parser

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader

import scala.util.{Try, Success, Failure}
import scalaz.{-\/, \/-, \/}

case class Message(userId: Long, geometry: Geometry)
case class ParseError(errorMessage: ParserErrorMessages)

object GeomUtils {
  val SRID = 4326

  def parseGeometry(geomWkt: String): Try[Geometry] = {
    val reader = new WKTReader
    Try {
      val geom = reader.read(geomWkt)
      geom.setSRID(SRID)
      geom
    }
  }
}

object MessageParser {
  //TODO: Extract to a properties file, would it make sense though?
  val separator = '|'

  def parse(message: String): \/[ParseError, Message] = {
    val fields = message.split(separator)
    if (emptyFields(fields)) { -\/(ParseError(EmptyMessage)) }
    else {
      parseFields(fields) match {
        case Success(message) => \/-(message)
        case Failure(ex) => -\/(ParseError(WrongFieldFormatMessage))
      }
    }
  }

  private def emptyFields(fields: Array[String]) = (fields.size == 1) && (fields(0).equals(""))

  private def parseFields(fields: Array[String]) = {
    for {
      userId <- parseUserId(fields(0))
      geom <- GeomUtils.parseGeometry(fields(1))
    } yield Message(userId, geom)
  }

  private def parseUserId(userId: String) = Try(userId.toLong)
}
