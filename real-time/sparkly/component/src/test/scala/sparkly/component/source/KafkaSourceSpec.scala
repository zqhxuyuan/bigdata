package sparkly.component.source

import sparkly.testing._
import sparkly.core._

class KafkaSourceSpec extends ComponentSpec with EmbeddedKafka {

  "Kafka source" should "stream message from a topic" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[KafkaSource].getName,
      name = "Kafka source",
      properties = Map (
        "Topics" -> "test-topic",
        "Group Id" -> "kafka-source-test",
        "Metadata broker list" -> kafkaServer.kafkaBroker
      ),
      outputs = Map (
        "Output" -> StreamConfiguration(mappedFeatures = Map("Message" -> "Message"))
      )
    )

    // When
    kafkaServer.createTopic("test-topic")
    val component = deployComponent(configuration)
    kafkaServer.send("test-topic", "Hello", "How are you?", "Does it work?")

    // Then
    eventually {
      component.outputs("Output").features should contain only (
        Map("Message" -> "Hello"),
        Map("Message" -> "How are you?"),
        Map("Message" -> "Does it work?")
      )
    }
  }

}
