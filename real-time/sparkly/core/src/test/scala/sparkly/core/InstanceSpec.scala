package sparkly.core

import org.scalatest._

class InstanceSpec extends FlatSpec with Matchers {

  "Instance" should "contain some mapped features" in {
    val instance = Instance("name" -> "Julie", "age" -> 30, "city" -> "Paris")
      .copy(inputMapper = Some(Mapper(namedFeatures = Map("id" -> "name"), listedFeatures = Map("values" -> List("age", "city", "gender")))))

    instance.inputFeature("id").asString should be ("Julie")
    instance.inputFeatures("values").asList should contain only (Feature(Some(30)), Feature(Some("Paris")), Feature(None))
  }

  "Instance" should "support new output feature" in {
    val instance = Instance()
      .copy(outputMapper = Some(Mapper(namedFeatures = Map("id" -> "name"))))
      .outputFeature("id", "Julie")

    instance.outputFeature("id").asString should be ("Julie")
  }

  "Instance" should "support new output features" in {
    val instance = Instance()
      .copy(outputMapper = Some(Mapper(listedFeatures = Map("values" -> List("age", "city", "gender")))))
      .outputFeatures("values", List(30, "Paris"))

    instance.outputFeatures("values").asList should contain only (Feature(Some(30)), Feature(Some("Paris")), Feature(None))
  }

  "Instance" should "support new output named features in batch" in {
    val instance = Instance()
      .copy(outputMapper = Some(Mapper(namedFeatures = Map("id" -> "username", "name" -> "first name"))))
      .outputFeatures("id" -> "jchanut", "name" -> "Julie")

    instance.rawFeatures should contain only ("username" -> "jchanut", "first name" -> "Julie")
  }

  "Instance" should "support input feature overriding" in {
    val instance = Instance("name" -> "Juli", "age" -> 29, "city" -> "Pari")
      .copy(inputMapper = Some(Mapper(namedFeatures = Map("id" -> "name"), listedFeatures = Map("values" -> List("age", "city", "gender")))))
      .inputFeature("id", "Julie")
      .inputFeatures("values", List(30, "Paris", "Female"))

    instance.inputFeature("id").asString should be ("Julie")
    instance.inputFeatures("values").asList should contain only (Feature(Some(30)), Feature(Some("Paris")), Feature(Some("Female")))
  }
}
