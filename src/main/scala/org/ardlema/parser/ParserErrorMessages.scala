package org.ardlema.parser

sealed trait ParserErrorMessages { val errorMessage: String }

case object EmptyMessage extends ParserErrorMessages { override val errorMessage = "Empty message" }
case object WrongFieldFormatMessage extends ParserErrorMessages { override val errorMessage = "Wrong field format" }
