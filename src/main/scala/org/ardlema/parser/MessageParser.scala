package org.ardlema.parser

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.WKTReader

import scala.util.{Try, Success, Failure}

case class Message(userId: Long, geometry: Geometry)
case class ParseError(errorMessage: ParserErrorMessages)

object MessageParser {
  //TODO: Extract to a properties file, would it make sense though?
  val separator = '|'
  val SRID = 4326

  def parse(message: String): Either[ParseError, Message] = {
    val fields = message.split(separator)
    if (emptyFields(fields)) Left(ParseError(EmptyMessage))
    else parseFields(fields) match {
      case Success(message) => Right(message)
      case Failure(ex) => Left(ParseError(WrongFieldFormatMessage))
    }
  }

  private def emptyFields(fields: Array[String]) = (fields.size == 1) && (fields(0).equals(""))

  private def parseFields(fields: Array[String]) = {
    for {
      userId <- parseUserId(fields(0))
      geom <- parseGeometry(fields(1))
    } yield Message(userId, geom)
  }

  private def parseUserId(userId: String) = Try(userId.toLong)

  private def parseGeometry(geomWkt: String) = {
    val reader = new WKTReader
    Try {
      val geom = reader.read(geomWkt)
      geom.setSRID(SRID)
      geom
    }
  }
}
