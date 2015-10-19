package sparkly.component.preprocess

import sparkly.testing._
import sparkly.core._

class ClassBalancerSpec extends ComponentSpec {

  "ClassBalancer" should "rebalance classes" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[ClassBalancer].getName,
      name = "ClassBalancer",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Class" -> "country")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("user" -> "user1", "country" -> "ES"),
      Instance("user" -> "user2", "country" -> "ES"),
      Instance("user" -> "user3", "country" -> "ES"),
      Instance("user" -> "user4", "country" -> "EN"),
      Instance("user" -> "user5", "country" -> "FR"),
      Instance("user" -> "user6", "country" -> "FR"),
      Instance("user" -> "user7", "country" -> "ES"),
      Instance("user" -> "user8", "country" -> "FR"),
      Instance("user" -> "user9", "country" -> "FR"),
      Instance("user" -> "user10", "country" -> "EN"),
      Instance("user" -> "user11", "country" -> "EN"),
      Instance("user" -> "user12", "country" -> "ES")
    )

    // Then
    eventually {
      val countries = component.outputs("Output").features.map(data => data("country").toString)
      countries.count(c => c == "EN") should be (3)
      countries.count(c => c == "ES") should be (3)
      countries.count(c => c == "FR") should be (3)
    }
  }

}
