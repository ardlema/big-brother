package org.ardlema.parser

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader

import scala.util.{Try, Success, Failure}
import scalaz.{-\/, \/-, \/}

case class Message(userId: Long, geometry: Geometry)
case class ParseError(errorMessage: ParserErrorMessages)

object GeomUtils {
  val SRID = 4326

  def parseGeometry(geomWkt: String) = {
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
    if (emptyFields(fields)) -\/(ParseError(EmptyMessage))
    else parseFields(fields) match {
      case Success(message) => \/-(message)
      case Failure(ex) => -\/(ParseError(WrongFieldFormatMessage))
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
