package sparkly.component.preprocess

import sparkly.core._
import sparkly.testing._

class LabelEncoderSpec extends ComponentSpec {

  "LabelEncoder" should "encode categorical label into discrete feature" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[LabelEncoder].getName,
      name = "LabelEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Label" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Discrete label" -> "Discrete label")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("user" -> "user1", "country" -> "ES"),
      Instance("user" -> "user2", "country" -> "EN"),
      Instance("user" -> "user3", "country" -> "IT"),
      Instance("user" -> "user4", "country" -> "PT"),
      Instance("user" -> "user5", "country" -> "PT"),
      Instance("user" -> "user6", "country" -> "EN"),
      Instance("user" -> "user7", "country" -> "FR")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("user" -> "user1", "country" -> "ES", "Discrete label" -> 0),
        Map("user" -> "user2", "country" -> "EN", "Discrete label" -> 1),
        Map("user" -> "user3", "country" -> "IT", "Discrete label" -> 2),
        Map("user" -> "user4", "country" -> "PT", "Discrete label" -> 3),
        Map("user" -> "user5", "country" -> "PT", "Discrete label" -> 3),
        Map("user" -> "user6", "country" -> "EN", "Discrete label" -> 1),
        Map("user" -> "user7", "country" -> "FR", "Discrete label" -> 4)
      )
    }
  }

}

