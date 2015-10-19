package sparkly.component.recommendation

import org.scalatest.time.{Millis, Span}
import sparkly.math.regression.RunningRmsd
import sparkly.testing.ComponentSpec
import sparkly.core._
import sparkly.component.source.dataset.MovieLensDataset

class CFRecommenderSpec extends ComponentSpec {

  "CFRecommender" should "train on movielens data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "CFRecommender",
      clazz = classOf[CFRecommender].getName,
      inputs = Map(
        "Ratings" -> StreamConfiguration(mappedFeatures = Map("User" -> "User", "Item" -> "Item", "Rating" -> "Rating")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("User" -> "User", "Item" -> "Item"))
      ),
      outputs = Map("Predictions" -> StreamConfiguration(mappedFeatures = Map("Prediction" -> "Prediction"))),
      properties = Map (
        "Minimum rating" -> "1.0",
        "Maximum rating" -> "5.0",
        "K" -> Math.floor(Math.sqrt(cores)).toInt.toString
      )
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Ratings").push(MovieLensDataset.trainDataset())

    Thread.sleep(15000)
    component.inputs("Predict").push(MovieLensDataset.testDataset())

    // Then
    eventually {
      val latestPredictions = component.outputs("Predictions").instances.map(i => (i.rawFeature("Prediction").asDouble, i.rawFeature("Rating").asDouble)).toList
      val rmsd = RunningRmsd(latestPredictions)
      rmsd.value should be < 1.0
    }
  }

}
