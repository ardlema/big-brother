package org.ardlema.parser

import org.scalatest.{ShouldMatchers, GivenWhenThen, FlatSpec}

class MessageParserTest extends FlatSpec with GivenWhenThen with ShouldMatchers {
  val properMessage = "1234|POLYGON(" +
    "(-3.9968132972717285 40.63518634434282," +
    "-3.9978432655334473 40.63451871274505," +
    "-3.9962339401245117 40.63422560408105," +
    "-3.9968132972717285 40.63518634434282))"


  behavior of "MessageParser"
  "A message" should "be parsed" in {
    val parsedMessage = MessageParser.parse(properMessage)
    parsedMessage.map(message => {
        message.userId should equal(1234)
        message.geometry.getNumPoints should equal(4)
        val Array(coord1, coord2, coord3, coord4) = message.geometry.getCoordinates
        coord1.x should equal(-3.9968132972717285)
        coord1.y should equal(40.63518634434282)
        coord2.x should equal(-3.9978432655334473)
        coord2.y should equal(40.63451871274505)
        coord3.x should equal(-3.9962339401245117)
        coord3.y should equal(40.63422560408105)
        coord4.x should equal(-3.9968132972717285)
        coord4.y should equal(40.63518634434282)
    })
  }

  "A message" should "not be parsed when is empty" in {
    val emptyMessage = ""
    val parsedMessage = MessageParser.parse(emptyMessage)

    parsedMessage.swap.map(e => e.errorMessage should be (EmptyMessage))
  }

  "A message" should "not be parsed when the user id has wrong type" in {
    val wrongIdMessage = "hello|POLYGON(" +
      "(-3.9968132972717285 40.63518634434282," +
      "-3.9978432655334473 40.63451871274505," +
      "-3.9962339401245117 40.63422560408105," +
      "-3.9968132972717285 40.63518634434282))"
    val parsedMessage = MessageParser.parse(wrongIdMessage)

    parsedMessage.swap.map(e => e.errorMessage should be (WrongFieldFormatMessage))
  }

  "A message" should "not be parsed when the it has a wrong location" in {
    val wrongIdMessage = "hello|wrongLocation"
    val parsedMessage = MessageParser.parse(wrongIdMessage)

    parsedMessage.swap.map(e => e.errorMessage should be (WrongFieldFormatMessage))
  }

  "A right message" should "be identified as proper message" in {
    MessageParser.properMessage(properMessage) should be (true)
  }

  "A wrong message" should "not be identified as proper message" in {
    val wrongMessage = "hello|wrongLocation"

    MessageParser.properMessage(wrongMessage) should be (false)
  }
}
