package sparkly.component.misc

import sparkly.testing._
import sparkly.core._

class JoinSpec extends ComponentSpec {

  "Join" should "do inner join" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Join].getName,
      name = "Join",
      properties = Map("Type" -> "Inner join"),
      inputs = Map (
        "Stream 1" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("name", "age"), "Non-join features" -> List("city", "country"))),
        "Stream 2" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("username", "age"), "Non-join features" -> List("validated")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Join and Non-join features" -> List("name", "age", "city", "country", "validated")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Stream 1").push (
      Instance("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "language" -> "en"),
      Instance("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "language" -> "fr"),
      Instance("name" -> "Julie", "age" -> 27, "city" -> "Nantes", "country" -> "France", "language" -> "bz")
    )
    component.inputs("Stream 2").push (
      Instance("username" -> "Pierre", "age" -> 27, "validated" -> true),
      Instance("username" -> "Julie", "age" -> 32, "validated" -> false),
      Instance("username" -> "Perceval", "age" -> 15, "validated" -> false)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "validated" -> true),
        Map("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "validated" -> false)
      )
    }
  }

  "Join" should "do right join" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Join].getName,
      name = "Join",
      properties = Map("Type" -> "Right join"),
      inputs = Map (
        "Stream 1" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("name", "age"), "Non-join features" -> List("city", "country"))),
        "Stream 2" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("username", "age"), "Non-join features" -> List("validated")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Join and Non-join features" -> List("name", "age", "city", "country", "validated")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Stream 1").push (
      Instance("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "language" -> "en"),
      Instance("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "language" -> "fr"),
      Instance("name" -> "Julie", "age" -> 27, "city" -> "Nantes", "country" -> "France", "language" -> "bz")
    )
    component.inputs("Stream 2").push (
      Instance("username" -> "Pierre", "age" -> 27, "validated" -> true),
      Instance("username" -> "Julie", "age" -> 32, "validated" -> false),
      Instance("username" -> "Perceval", "age" -> 15, "validated" -> false)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "validated" -> true),
        Map("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "validated" -> false),
        Map("name" -> "Perceval", "age" -> 15, "city" -> null, "country" -> null, "validated" -> false)
        )
    }
  }

  "Join" should "do left join" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[Join].getName,
      name = "Join",
      properties = Map("Type" -> "Left join"),
      inputs = Map (
        "Stream 1" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("name", "age"), "Non-join features" -> List("city", "country"))),
        "Stream 2" -> StreamConfiguration(selectedFeatures = Map("Join features" -> List("username", "age"), "Non-join features" -> List("validated")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Join and Non-join features" -> List("name", "age", "city", "country", "validated")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Stream 1").push (
      Instance("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "language" -> "en"),
      Instance("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "language" -> "fr"),
      Instance("name" -> "Julie", "age" -> 27, "city" -> "Nantes", "country" -> "France", "language" -> "bz")
    )
    component.inputs("Stream 2").push (
      Instance("username" -> "Pierre", "age" -> 27, "validated" -> true),
      Instance("username" -> "Julie", "age" -> 32, "validated" -> false),
      Instance("username" -> "Perceval", "age" -> 15, "validated" -> false)
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "validated" -> true),
        Map("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "validated" -> false),
        Map("name" -> "Julie", "age" -> 27, "city" -> "Nantes", "country" -> "France", "validated" -> null)
      )
    }
  }
}
