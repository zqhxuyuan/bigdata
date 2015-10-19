package sparkly.core

import org.scalatest._

import sparkly.core.PropertyType._

class PropertySpec extends FlatSpec with Matchers {

  "Property" should "have default value" in {
    Property(DECIMAL, defaultValue = Some(0.25)).as[Double] should be(0.25)
    Property(DECIMAL, defaultValue = Some(0.25), selectedValue = Some("0.42")).as[Double] should be(0.42)
  }

  "Property" should "be converted to simple type" in {
    Property(STRING, selectedValue = Some("bar")).as[String] should be ("bar")
    Property(INTEGER,selectedValue = Some("42")).as[Int] should be (42)
    Property(DECIMAL, selectedValue = Some("0.25")).as[Double] should be (0.25)
    Property(BOOLEAN, selectedValue = Some("true")).as[Boolean] should be (true)
  }

  "Property" should "have optional value" in {
    Property(STRING, selectedValue = Some("42")).isDefined should be (true)
    Property(STRING, selectedValue = Some("42")).isEmpty should be (false)

    Property(STRING).isDefined should be (false)
    Property(STRING).isEmpty should be (true)
  }
}
