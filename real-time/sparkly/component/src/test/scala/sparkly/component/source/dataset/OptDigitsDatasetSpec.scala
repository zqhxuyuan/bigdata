package sparkly.component.source.dataset

import sparkly.core.{StreamConfiguration, ComponentConfiguration}
import sparkly.testing.ComponentSpec
import breeze.linalg.DenseVector

class OptDigitsDatasetSpec extends ComponentSpec {

  "OptDigitsDataset" should "stream optdigits data" in {
    val configuration = ComponentConfiguration(
      clazz = classOf[OptDigitsDataset].getName,
      name = "OptDigitsDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))),
      properties = Map(
        "Throughput (instance/second)" -> "5000",
        "Loop" -> "false"
      )
    )

    val component = deployComponent(configuration)

    eventually {
      component.outputs("Instances").instances.size should be (5620)
      component.outputs("Instances").instances.foreach{ instance =>
        instance.rawFeatures.contains("Label") should be (true)
        instance.rawFeatures("Features").asInstanceOf[DenseVector[Double]].activeSize should be (64)
      }
    }
  }

}
