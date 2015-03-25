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
    else parseUserId(fields(0)) match {
      case Success(id) => Right(Message(id))
      case Failure(ex) => Left(ParseError(WrongUserIdMessage))
    }
  }

  private def emptyFields(fields: Array[String]) = (fields.size == 1) && (fields(0).equals(""))

  private def parseUserId(userId: String) = Try(userId.toLong)
}
