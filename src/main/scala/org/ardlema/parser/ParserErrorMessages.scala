package org.ardlema.parser

sealed trait ParserErrorMessages { val errorMessage: String }

case object EmptyMessage extends ParserErrorMessages { override val errorMessage = "Empty message" }
case object WrongUserIdMessage extends ParserErrorMessages { override val errorMessage = "Wrong user id" }
