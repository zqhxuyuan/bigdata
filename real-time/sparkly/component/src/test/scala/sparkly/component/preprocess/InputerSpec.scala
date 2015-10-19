package sparkly.component.preprocess

import sparkly.core._
import sparkly.testing._

class InputerSpec extends ComponentSpec {

  "Inputer" should "impute missing value with mean" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Inputer].getName,
      name = "Inputer",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      ), properties = Map (
        "Missing" -> "0",
        "Strategy" -> "Mean"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (
      Instance("f1" -> 0, "f2" -> 3, "f3" -> 4),
      Instance("f1" -> 6, "f2" -> 0, "f3" -> 6),
      Instance("f1" -> 3, "f2" -> 3, "f3" -> 2),
      Instance("f1" -> 6, "f2" -> 3, "f3" -> null)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("f1" -> 5.0, "f2" -> 3.0, "f3" -> 4.0),
        Map("f1" -> 6.0, "f2" -> 3.0, "f3" -> 6.0),
        Map("f1" -> 3.0, "f2" -> 3.0, "f3" -> 2.0),
        Map("f1" -> 6.0, "f2" -> 3.0, "f3" -> 4.0)
      )
    }
  }

  "Inputer" should "impute missing value with default value" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Inputer].getName,
      name = "Inputer",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      ), properties = Map (
        "Missing" -> "0",
        "Strategy" -> "Default",
        "Default value" -> "42.0"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (
      Instance("f1" -> 0, "f2" -> 3, "f3" -> 4),
      Instance("f1" -> 6, "f2" -> 0, "f3" -> 6),
      Instance("f1" -> 3, "f2" -> 3, "f3" -> 2),
      Instance("f1" -> 6, "f2" -> 3, "f3" -> null)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("f1" -> 42.0, "f2" -> 3.0, "f3" -> 4.0),
        Map("f1" -> 6.0, "f2" -> 42.0, "f3" -> 6.0),
        Map("f1" -> 3.0, "f2" -> 3.0, "f3" -> 2.0),
        Map("f1" -> 6.0, "f2" -> 3.0, "f3" -> 42.0)
      )
    }
  }

  "Inputer" should "filter instance with missing value" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Inputer].getName,
      name = "Inputer",
      inputs = Map (
        "Input" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("f1", "f2", "f3")))
      ), properties = Map (
        "Missing" -> "0",
        "Strategy" -> "Filter"
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Input").push (
      Instance("f1" -> 0, "f2" -> 3, "f3" -> 4),
      Instance("f1" -> 6, "f2" -> 0, "f3" -> 6),
      Instance("f1" -> 3, "f2" -> 3, "f3" -> 2),
      Instance("f1" -> 6, "f2" -> 3, "f3" -> null)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("f1" -> 3.0, "f2" -> 3.0, "f3" -> 2.0)
      )
    }
  }
}
