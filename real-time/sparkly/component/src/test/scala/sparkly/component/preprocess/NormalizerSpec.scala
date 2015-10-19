package sparkly.component.preprocess

import sparkly.testing._
import sparkly.core._
import breeze.linalg.DenseVector

class NormalizerSpec extends ComponentSpec {

  "Normalizer" should "normalize features" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Normalizer].getName,
      name = "Normalizer",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Normalized features" -> "Normalized features"))
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (
      Instance("Features" -> DenseVector(Array(0.0, 3.0, 4.0))),
      Instance("Features" -> DenseVector(Array(6.0, 0.0, 8.0)))
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Features" -> DenseVector(Array(0.0, 3.0, 4.0)), "Normalized features" -> DenseVector(Array(0.0, 0.6, 0.8))),
        Map("Features" -> DenseVector(Array(6.0, 0.0, 8.0)), "Normalized features" -> DenseVector(Array(0.6, 0.0, 0.8)))
      )
    }
  }
}
