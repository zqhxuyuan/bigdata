package sparkly.component.misc

import sparkly.testing._
import sparkly.core._

class BasicFilterSpec extends ComponentSpec {

  "Basic filter" should "filter null value" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> "IS NULL")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push (
      Instance("name" -> "Juju", "age" -> null),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien")
    )

    // Then
    eventually {
      component.outputs("Out").features should contain only (
        Map("name" -> "Juju", "age" -> null),
        Map("name" -> "Fabien")
      )
    }
  }

  "Basic filter" should "filter out null value" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> "IS NOT NULL")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push (
      Instance("name" -> "Juju", "age" -> null),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien")
    )

    // Then
    eventually {
      component.outputs("Out").features should contain only (
        Map("name" -> "Pierre", "age" -> 27)
      )
    }
  }

  "Basic filter" should "filter value based on operand" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[BasicFilter].getName,
      name = "BasicFilter",
      inputs = Map (
        "In" -> StreamConfiguration(mappedFeatures = Map("Feature" -> "age"))
      ),
      outputs = Map (
        "Out" -> StreamConfiguration()
      ),
      properties = Map ("Operator" -> ">=", "Operand" -> "33")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("In").push (
      Instance("name" -> "Juju", "age" -> 33),
      Instance("name" -> "Pierre", "age" -> 27),
      Instance("name" -> "Fabien", "age" -> 29)
    )

    // Then
    eventually {
      component.outputs("Out").features should contain only (
        Map("name" -> "Juju", "age" -> 33)
      )
    }
  }
}
