package sparkly.component.source.dataset

import sparkly.core.{StreamConfiguration, ComponentConfiguration}
import sparkly.testing.ComponentSpec

class HousingDatasetSpec extends ComponentSpec {

  "HousingDataset" should "stream housing data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[HousingDataset].getName,
      name = "HousingDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = HousingDataset.featureNames.map(name => (name, name)).toMap)),
      properties = Map(
        "Throughput (instance/second)" -> "5000",
        "Loop" -> "false"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (506)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.keys should contain only (HousingDataset.featureNames: _*)
      }
    }
  }

}
