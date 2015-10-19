package sparkly.component.misc

import sparkly.testing._
import sparkly.core._
import breeze.linalg.DenseVector

class VectorExtractorSpec extends ComponentSpec {

  "VectorExtractor" should "extract values from vectors" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[VectorExtractor].getName,
      name = "VectorExtractor",
      inputs = Map (
        "Input" -> StreamConfiguration(mappedFeatures= Map("Vector" -> "Vector"))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Input").push (
      Instance("Vector" -> DenseVector(1.2, 3.4, 5.6)),
      Instance("Vector" -> DenseVector(7.8, 10.0, 12.2))
    )
    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Vector" -> DenseVector(1.2, 3.4, 5.6), "f1" -> 1.2, "f2" -> 3.4, "f3" -> 5.6),
        Map("Vector" -> DenseVector(7.8, 10.0, 12.2), "f1" -> 7.8, "f2" -> 10.0, "f3" -> 12.2)
      )
    }
  }

}
