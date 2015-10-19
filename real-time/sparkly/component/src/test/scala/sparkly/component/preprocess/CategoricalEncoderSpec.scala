package sparkly.component.preprocess

import sparkly.core._
import sparkly.testing._
import breeze.linalg.DenseVector

class CategoricalEncoderSpec extends ComponentSpec {

  "CategoricalEncoder" should "encode categorical feature into double array" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[CategoricalEncoder].getName,
      name = "CategoricalEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Encoded features" -> "Encoded features")))
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
        Map("user" -> "user1", "country" -> "ES", "Encoded features" -> DenseVector(Array(0.0, 0.0, 0.0, 0.0, 0.0))),
        Map("user" -> "user2", "country" -> "EN", "Encoded features" -> DenseVector(Array(0.0, 1.0, 0.0, 0.0, 0.0))),
        Map("user" -> "user3", "country" -> "IT", "Encoded features" -> DenseVector(Array(0.0, 0.0, 1.0, 0.0, 0.0))),
        Map("user" -> "user4", "country" -> "PT", "Encoded features" -> DenseVector(Array(0.0, 0.0, 0.0, 1.0, 0.0))),
        Map("user" -> "user5", "country" -> "PT", "Encoded features" -> DenseVector(Array(0.0, 0.0, 0.0, 1.0, 0.0))),
        Map("user" -> "user6", "country" -> "EN", "Encoded features" -> DenseVector(Array(0.0, 1.0, 0.0, 0.0, 0.0))),
        Map("user" -> "user7", "country" -> "FR", "Encoded features" -> DenseVector(Array(0.0, 0.0, 0.0, 0.0, 1.0)))
      )
    }
  }

  "CategoricalEncoder" should "add 0 filled array when there's too much categories feature" in {
    // Given
    val configuration = ComponentConfiguration(
      clazz = classOf[CategoricalEncoder].getName,
      name = "CategoricalEncoder",
      inputs = Map("Input" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "country"))),
      outputs = Map("Output" -> StreamConfiguration(mappedFeatures = Map("Encoded features" -> "Encoded features")))
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push(
      Instance("user" -> "user1", "country" -> "ES"),
      Instance("user" -> "user2", "country" -> "EN"),
      Instance("user" -> "user3", "country" -> "IT"),
      Instance("user" -> "user4", "country" -> "ES"),
      Instance("user" -> "user5", "country" -> "EN")
    )

    // Then
    eventually {
      component.outputs("Output").features contains only (
        Map("user" -> "user1", "country" -> "ES", "Encoded features" -> DenseVector(Array(1.0, 0.0))),
        Map("user" -> "user2", "country" -> "EN", "Encoded features" -> DenseVector(Array(0.0, 1.0))),
        Map("user" -> "user3", "country" -> "IT", "Encoded features" -> DenseVector(Array(0.0, 0.0))),
        Map("user" -> "user4", "country" -> "ES", "Encoded features" -> DenseVector(Array(1.0, 0.0))),
        Map("user" -> "user5", "country" -> "EN", "Encoded features" -> DenseVector(Array(0.0, 1.0)))
      )
    }
  }
}

