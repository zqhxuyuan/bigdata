package sparkly.component.misc

import sparkly.testing._
import sparkly.core._

class StreamingSqlSpec extends ComponentSpec {

  "Streaming SQL" should "execute sql query on single stream" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[StreamingSql].getName,
      name = "StreamingSql",
      properties = Map("Query" -> "SELECT name, country FROM stream1"),
      inputs = Map (
        "Stream1" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("name", "age", "city", "country", "language")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("extracted_name", "extracted_country")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Stream1").push (
      Instance("name" -> "Pierre", "age" -> 27, "city" -> "Paris", "country" -> "France", "language" -> "en"),
      Instance("name" -> "Julie", "age" -> 32, "city" -> "Paris", "country" -> "France", "language" -> "fr"),
      Instance("name" -> "Fabien", "age" -> 29, "city" -> "Nantes", "country" -> "France", "language" -> "bz")
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("extracted_name" -> "Pierre", "extracted_country" -> "France"),
        Map("extracted_name" -> "Julie", "extracted_country" -> "France"),
        Map("extracted_name" -> "Fabien", "extracted_country" -> "France")
      )
    }
  }

  "Streaming SQL" should "execute sql query on multiple streams" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[StreamingSql].getName,
      name = "StreamingSql",
      properties = Map("Query" ->
        """
          |SELECT MAX(stream1.temperature), MAX(stream2.humidity), stream1.location FROM stream1
          |LEFT JOIN stream2 on stream1.location = stream2.location
          |GROUP BY stream1.location
        """.stripMargin),
      inputs = Map (
        "Stream1" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("temperature", "location", "sensor_id"))),
        "Stream2" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("humidity", "location", "sensor_id")))
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(selectedFeatures = Map("Fields" -> List("temperature", "humidity", "location")))
      )
    )

    // When
    val component = deployComponent(configuration)

    component.inputs("Stream1").push (
      Instance("temperature" -> 9, "location" -> "Paris", "sensor_id" -> "paris_0"),
      Instance("temperature" -> 8, "location" -> "Paris", "sensor_id" -> "paris_1"),
      Instance("temperature" -> 18, "location" -> "Madrid", "sensor_id" -> "madrid_0")
    )
    component.inputs("Stream2").push (
      Instance("humidity" -> 92, "location" -> "Paris", "sensor_id" -> "paris_hydro_0")
    )

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("temperature" -> "9", "humidity" -> "92", "location" -> "Paris"),
        Map("temperature" -> "18", "humidity" -> null, "location" -> "Madrid")
      )
    }
  }
}
