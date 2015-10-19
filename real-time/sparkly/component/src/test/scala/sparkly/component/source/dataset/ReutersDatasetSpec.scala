package sparkly.component.source.dataset

import sparkly.testing.ComponentSpec
import sparkly.core.{StreamConfiguration, ComponentConfiguration}

class ReutersDatasetSpec extends ComponentSpec {

  "ReutersDataset" should "stream reuters data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[ReutersDataset].getName,
      name = "ReutersDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = Map("Topic" -> "Topic", "Text" -> "Text"))),
      properties = Map (
        "Throughput (instance/second)" -> "5000",
        "Loop" -> "false"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (2215)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures("Topic").asInstanceOf[String] should not be (null)
        instance.rawFeatures("Text").asInstanceOf[String] should not be (null)
      }
    }
  }

}
