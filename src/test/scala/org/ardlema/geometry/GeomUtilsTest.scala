package org.ardlema.geometry

import org.scalatest._

class GeomUtilsTest extends FlatSpec with GivenWhenThen with ShouldMatchers {

  //TODO: Move this stuff to a trait
  val firstIntersectedGeom = "POLYGON(" +
    "(-3.9968132972717285 40.63518634434282," +
    "-3.9978432655334473 40.63451871274505," +
    "-3.9962339401245117 40.63422560408105," +
    "-3.9968132972717285 40.63518634434282))"
  val secondIntersectedGeom = "POLYGON(" +
    "(-3.99627149105072 40.63487129379284," +
    "-3.9971405267715454 40.634036749314504," +
    "-3.9955633878707886 40.63410595583863," +
    "-3.99627149105072 40.63487129379284))"
  val thirdIntersectedGeom = "POLYGON(" +
    "(-3.9970600605010986 40.635054485087004," +
    "-3.9966416358947754 40.63398382662999," +
    "-3.9955151081085205 40.63474509505337," +
    "-3.9970600605010986 40.635054485087004))"
  val notIntersectedGeom = "POLYGON(" +
    "(-4.00402307510376 40.63227151343767," +
    "-4.004087448120117 40.630643060882576," +
    "-4.002285003662109 40.63150614568316," +
    "-4.00402307510376 40.63227151343767))"
  val geom1 = GeomUtils.parseGeometry(firstIntersectedGeom).toOption.get
  val geom2 = GeomUtils.parseGeometry(secondIntersectedGeom).toOption.get
  val geom3 = GeomUtils.parseGeometry(thirdIntersectedGeom).toOption.get
  val geom4 = GeomUtils.parseGeometry(notIntersectedGeom).toOption.get
  val intersectedGeomsList = List(geom1, geom2, geom3)
  val nonIntersectedGeomsList = List(geom1, geom2, geom4)

  "The GeomUtils" should "return true when a list of geoms intersect to each other" in {
    GeomUtils.intersectAll(intersectedGeomsList) should be(true)
  }

  it should "return false when a geom within a list of geoms does not intersect to each other" in {
    GeomUtils.intersectAll(nonIntersectedGeomsList) should be(false)
  }

  it should "intersect all the geometries" in {
    val expectedIntersection = geom1.intersection(geom2).intersection(geom3)

    GeomUtils.intersectGeoms(List(geom1, geom2, geom3)) should be(expectedIntersection)
  }
}
