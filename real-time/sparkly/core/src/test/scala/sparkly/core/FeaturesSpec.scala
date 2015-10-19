package sparkly.core


import breeze.linalg.DenseVector
import java.util.Date
import org.scalatest._

class FeaturesSpec extends FlatSpec with Matchers {

  "Feature" should "be optional" in {
    Feature(None).isEmpty should be (true)
    Feature(None).isDefined should be (false)

    Feature("42").isEmpty should be (false)
    Feature("42").isDefined should be (true)

    Feature(None).getOrElse(1.5) should be (1.5)
  }


  "Feature List" should "be converted" in {
    val list = FeatureList(List( Feature("42"), Feature(43.43), Feature(44), Feature(45L), Feature(true)))

    list.asDoubleList should contain inOrderOnly (42.0, 43.43, 44.0, 45.0, 1.0)
  }

  "String feature" should "should be converted to other types" in {
    Feature("42").asString should be ("42")
    Feature("42.42").asDouble should be (42.42)
    Feature("42").asInt should be (42)
    Feature("42").asLong should be (42L)
    Feature("1610-07-10T00:00:00.000+00:00").asDate.getTime should be (-11344060800000L)
    Feature("true").asBoolean should be (true)
  }

  "Double feature" should "should be converted to other types" in {
    Feature(42.42).asString should be ("42.42")
    Feature(42.42).asDouble should be (42.42)
    Feature(42.42).asInt should be (42)
    Feature(42.42).asLong should be (42L)
    intercept[IllegalArgumentException] {Feature(42.42).asDate}
    Feature(42.42).asBoolean should be (true)
    intercept[IllegalArgumentException] {Feature(42.42).asVector}
  }

  "Int feature" should "should be converted to other types" in {
    Feature(42).asString should be ("42")
    Feature(42).asDouble should be (42.0)
    Feature(42).asInt should be (42)
    Feature(42).asLong should be (42L)
    Feature(0).asDate should be (new Date(0))
    Feature(42).asBoolean should be (true)
    intercept[IllegalArgumentException] {Feature(42).asVector}
  }

  "Long feature" should "should be converted to other types" in {
    val now = new Date()

    Feature(42L).asString should be ("42")
    Feature(42L).asDouble should be (42.0)
    Feature(42L).asInt should be (42)
    Feature(42L).asLong should be (42L)
    Feature(now.getTime).asDate should be (now)
    Feature(42L).asBoolean should be (true)
    intercept[IllegalArgumentException] {Feature(42L).asVector}
  }

  "Date feature" should "should be converted to other types" in {
    val beginningOfTime = new Date(0)

    Feature(beginningOfTime).asString should be ("1970-01-01T00:00:00.000Z")
    intercept[IllegalArgumentException] {Feature(beginningOfTime).asDouble}
    Feature(beginningOfTime).asInt should be (0)
    Feature(beginningOfTime).asLong should be (0L)
    Feature(beginningOfTime).asDate should be (beginningOfTime)
    intercept[IllegalArgumentException] {Feature(beginningOfTime).asBoolean}
    intercept[IllegalArgumentException] {Feature(beginningOfTime).asVector}
  }

  "Boolean feature" should "should be converted to other types" in {
    Feature(true).asString should be ("true")
    Feature(true).asDouble should be (1.0)
    Feature(true).asInt should be (1)
    Feature(true).asLong should be (1L)
    Feature(true).asBoolean should be (true)
    intercept[IllegalArgumentException] {Feature(true).asDate}
    intercept[IllegalArgumentException] {Feature(true).asVector}
  }

  "Vector feature" should "should not be converted to other types" in {
    val vector = DenseVector(Array.fill(4)(0.0))
    val feature = Feature(vector)
    intercept[IllegalArgumentException] {feature.asString}
    intercept[IllegalArgumentException] {feature.asDouble}
    intercept[IllegalArgumentException] {feature.asInt}
    intercept[IllegalArgumentException] {feature.asLong}
    intercept[IllegalArgumentException] {feature.asBoolean}
    intercept[IllegalArgumentException] {feature.asDate}
    feature.asVector should be (vector)
  }
}