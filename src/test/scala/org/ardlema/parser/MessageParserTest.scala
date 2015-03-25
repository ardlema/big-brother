package org.ardlema.parser

import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, GivenWhenThen, FlatSpec}

class MessageParserTest extends FlatSpec with GivenWhenThen with Eventually with Matchers {

  behavior of "MessageParser"
  "A message" should "be parsed" in {
    val message = "1234|POLYGON(" +
      "(-3.9968132972717285 40.63518634434282," +
      "-3.9978432655334473 40.63451871274505," +
      "-3.9962339401245117 40.63422560408105," +
      "-3.9968132972717285 40.63518634434282))"
    val parsedMessage = MessageParser.parse(message)

    parsedMessage.fold(e => fail, r => r.userId should be (1234))
    //TODO: Parse the location
    //parsedMessage.location should be()
  }

  "A message" should "be not parsed when is empty" in {
    val emptyMessage = ""
    val parsedMessage = MessageParser.parse(emptyMessage)

    parsedMessage.fold(e => e.errorMessage should be (EmptyMessage), r => fail)
    //TODO: Parse the location
    //parsedMessage.location should be()
  }

  "A message" should "be not parsed when the user id has wrong type" in {
    val wrongIdMessage = "hello|POLYGON(" +
      "(-3.9968132972717285 40.63518634434282," +
      "-3.9978432655334473 40.63451871274505," +
      "-3.9962339401245117 40.63422560408105," +
      "-3.9968132972717285 40.63518634434282))"
    val parsedMessage = MessageParser.parse(wrongIdMessage)

    parsedMessage.fold(e => e.errorMessage should be (WrongUserIdMessage), r => fail)
    //TODO: Parse the location
    //parsedMessage.location should be()
  }
}
