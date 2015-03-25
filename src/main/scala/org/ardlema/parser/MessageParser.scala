package org.ardlema.parser

import scala.util.{Try, Success, Failure}

case class Message(userId: Long)
case class ParseError(errorMessage: ParserErrorMessages)

object MessageParser {
  //TODO: Extract to a properties file, would it make sense?
  val separator = '|'

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
    } yield Message(userId)
  }

  private def parseUserId(userId: String) = Try(userId.toLong)
}
