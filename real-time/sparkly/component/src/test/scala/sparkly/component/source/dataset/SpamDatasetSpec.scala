package sparkly.component.source.dataset

import sparkly.testing._
import sparkly.core._

class SpamDatasetSpec extends ComponentSpec {

  "SpamDataset" should "stream spam data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[SpamDataset].getName,
      name = "SpamDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))),
      properties = Map (
        "Throughput (instance/second)" -> "6000",
        "Loop" -> "false"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (4601)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only ("Label", "Features")
      }
    }
  }

  "SpamDataset" should "loop spam data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[SpamDataset].getName,
      name = "SpamDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))),
      properties = Map(
        "Throughput (instance/second)" -> "6000",
        "Loop" -> "True"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be > 5000
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only ("Label", "Features")
      }
    }
  }
}
