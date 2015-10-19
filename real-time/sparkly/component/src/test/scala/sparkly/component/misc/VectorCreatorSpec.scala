package sparkly.component.misc

import sparkly.testing.ComponentSpec
import sparkly.core.{Instance, StreamConfiguration, ComponentConfiguration}
import breeze.linalg.DenseVector

class VectorCreatorSpec extends ComponentSpec {

  "VectorCreator" should "create vector from feature" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[VectorCreator].getName,
      name = "VectorCreator",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("double", "string", "vec")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Vector" -> "Vector"))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Input").push (
      Instance("double" -> 4.2, "string" -> "27", "vec" -> DenseVector(1.2, 3.4, 5.6)),
      Instance("double" -> null, "string" -> "34", "vec" -> DenseVector(1.2, 3.4, 5.6)),
      Instance("double" -> 5.8, "string" -> "parse error", "vec" -> DenseVector(1.2, 3.4, 5.6))
    )
    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("double" -> 4.2, "string" -> "27", "vec" -> DenseVector(1.2, 3.4, 5.6), "Vector" -> DenseVector(4.2, 27.0, 1.2, 3.4, 5.6)),
        Map("double" -> null, "string" -> "34", "vec" -> DenseVector(1.2, 3.4, 5.6), "Vector" -> DenseVector(0.0, 34.0, 1.2, 3.4, 5.6)),
        Map("double" -> 5.8, "string" -> "parse error", "vec" -> DenseVector(1.2, 3.4, 5.6), "Vector" -> DenseVector(5.8, 0.0, 1.2, 3.4, 5.6))
      )
    }
  }

}
