package sparkly.component.store

import sparkly.testing._
import sparkly.core._

class InMemoryStoreSpec extends ComponentSpec {

  "In memory store" should "do CRUD operations" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "In memory store",
      clazz = classOf[InMemoryStore].getName,
      inputs = Map (
        "Update" -> StreamConfiguration(mappedFeatures = Map("Id" -> "user-id"), selectedFeatures = Map("Features" -> List("firstname", "age"))),
        "Query" -> StreamConfiguration(mappedFeatures = Map("Id" -> "user-id"))
      ),
      outputs = Map (
        "Query result" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("firstname", "age")))
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Update").push(
      Instance("user-id" -> 0, "firstname" -> "Pierre", "age" -> 27),
      Instance("user-id" -> 1, "firstname" -> "Julie", "age" -> 33),
      Instance("user-id" -> 1, "firstname" -> "Juju", "age" -> 33),
      Instance("user-id" -> 0, "firstname" -> "Pierre", "age" -> 28)
    )

    component.inputs("Query").push(
      Instance("user-id" -> 0),
      Instance("user-id" -> 1)
    )

    // Then
    eventually {
      component.outputs("Query result").features should contain only (
        Map("user-id" -> 0, "firstname" -> "Pierre", "age" -> 28),
        Map("user-id" -> 1, "firstname" -> "Juju", "age" -> 33)
      )
    }

  }
}
