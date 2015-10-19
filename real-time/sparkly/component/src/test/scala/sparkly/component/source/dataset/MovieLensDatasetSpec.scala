package sparkly.component.source.dataset

import sparkly.testing._
import sparkly.core._


class MovieLensDatasetSpec extends ComponentSpec {

  "MovieLensDataset" should "stream movielens data" in {
    // Given
    val configuration = ComponentConfiguration (
      clazz = classOf[MovieLensDataset].getName,
      name = "MovieLensDataset",
      outputs = Map("Instances" -> StreamConfiguration(mappedFeatures = Map("Label" -> "Label", "Features" -> "Features"))),
      properties = Map(
        "Throughput (instance/second)" -> "20000",
        "Loop" -> "false",
        "size" -> "1m"
      )
    )

    // When
    MovieLensDataset.getFile("1m")
    val component = deployComponent(configuration)

    // Then
    eventually {
      component.outputs("Ratings").size should be > 20000
    }
  }

}
