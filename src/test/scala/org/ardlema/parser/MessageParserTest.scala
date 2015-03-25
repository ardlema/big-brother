package org.ardlema.parser

import org.scalatest.concurrent.Eventually
import org.scalatest.{ShouldMatchers, GivenWhenThen, FlatSpec}

class MessageParserTest extends FlatSpec with GivenWhenThen with Eventually with ShouldMatchers {

  behavior of "MessageParser"
  "A message" should "be parsed" in {
    val message = "1234|POLYGON(" +
      "(-3.9968132972717285 40.63518634434282," +
      "-3.9978432655334473 40.63451871274505," +
      "-3.9962339401245117 40.63422560408105," +
      "-3.9968132972717285 40.63518634434282))"
    val parsedMessage = MessageParser.parse(message)

    parsedMessage.fold(e => fail, {
      r =>
        r.userId should equal(1234)
        r.geometry.getNumPoints should equal(4)
        val Array(coord1, coord2, coord3, coord4) = r.geometry.getCoordinates
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

    parsedMessage.fold(e => e.errorMessage should be (EmptyMessage), r => fail)
  }

  "A message" should "not be parsed when the user id has wrong type" in {
    val wrongIdMessage = "hello|POLYGON(" +
      "(-3.9968132972717285 40.63518634434282," +
      "-3.9978432655334473 40.63451871274505," +
      "-3.9962339401245117 40.63422560408105," +
      "-3.9968132972717285 40.63518634434282))"
    val parsedMessage = MessageParser.parse(wrongIdMessage)

    parsedMessage.fold(e => e.errorMessage should be (WrongFieldFormatMessage), r => fail)
  }

  "A message" should "not be parsed when the it has a wrong location" in {
    val wrongIdMessage = "hello|wrongLocation"
    val parsedMessage = MessageParser.parse(wrongIdMessage)

    parsedMessage.fold(e => e.errorMessage should be (WrongFieldFormatMessage), r => fail)
  }
}
