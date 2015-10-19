package sparkly.component.source.dataset

import sparkly.core.{StreamConfiguration, ComponentConfiguration}
import sparkly.testing.ComponentSpec

class IrisDatasetSpec extends ComponentSpec {

  "IrisDataset" should "stream iris data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[IrisDataset].getName,
      name = "IrisDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = IrisDataset.featureNames.map(name => (name, name)).toMap)),
      properties = Map (
        "Throughput (instance/second)" -> "5000",
        "Loop" -> "false"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (150)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only (IrisDataset.featureNames: _*)
      }
    }
  }

}
