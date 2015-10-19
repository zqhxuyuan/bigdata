package sparkly.component.writer

import sparkly.testing._
import sparkly.core._

class KafkaWriterSpec extends ComponentSpec with EmbeddedKafka {

  "Kafka writer" should "write features to kafka" in {

    val configuration = ComponentConfiguration (
      name = "Kafka writer",
      clazz = classOf[KafkaWriter].getName,
      inputs = Map (
        "In" -> StreamConfiguration(selectedFeatures = Map("Features" -> List("stationid", "timestamp", "temperature")))
      ),
      properties = Map (
        "Topic" -> "sensor",
        "Metadata broker list" -> kafkaServer.kafkaBroker,
        "Serializer" -> "Json",
        "Max retry" -> "10",
        "Retry backoff (ms)" -> "500",
        "Parallelism" -> "1"
      )
    )

    // When
    val kafkaData = kafkaServer.listen("sensor", "test")

    val component = deployComponent(configuration)
    component.inputs("In").push(
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 8),
      Instance("stationid" -> "0", "timestamp" -> "2015-02-19 21:47:28", "temperature" -> 9),
      Instance("stationid" -> "1", "timestamp" -> "2015-02-19 21:47:18", "temperature" -> 19)
    )

    // Then
    eventually {
      kafkaData.map(bytes => new String(bytes)) should contain only (
        """{"stationid":"0","timestamp":"2015-02-19 21:47:18","temperature":8}""",
        """{"stationid":"0","timestamp":"2015-02-19 21:47:28","temperature":9}""",
        """{"stationid":"1","timestamp":"2015-02-19 21:47:18","temperature":19}"""
      )
    }
  }
}
