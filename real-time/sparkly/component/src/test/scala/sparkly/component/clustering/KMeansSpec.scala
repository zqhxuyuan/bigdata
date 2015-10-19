package sparkly.component.clustering

import sparkly.component.source.dataset.SeedsDataset
import sparkly.core._
import sparkly.testing._

class KMeansSpec extends ComponentSpec {

  "Kmeans" should "train on seeds data" in {
    // Given
    val configuration = ComponentConfiguration (
      name = "Kmeans",
      clazz = classOf[KMeans].getName,
      inputs = Map (
        "Train" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features")),
        "Predict" -> StreamConfiguration(mappedFeatures = Map("Features" -> "Features"))
      ),
      outputs = Map("Predicted" -> StreamConfiguration(mappedFeatures = Map("Cluster" -> "Cluster"))),
      properties = Map ("Clusters" -> "3")
    )

    // When
    val component = deployComponent(configuration)
    component.inputs("Train").push(2000, SeedsDataset.iterator())
    component.inputs("Predict").push(2000, SeedsDataset.iterator())

    // Then
    eventually {
      val predictions = component.outputs("Predicted").instances.map(i => (i.rawFeature("Variety").asInt, i.rawFeature("Cluster").asInt)).toList
      accuracy(predictions) should be > 0.80
    }
  }

  private def accuracy(predictions: List[(Int, Int)]): Double = {
    val assignments = predictions.groupBy(_._1).map{ case (variety, values) =>
      val clusters = values.map(_._2).toList
      val assignment = mostFrequent(clusters)
      (assignment, variety)
    }.toMap

    val success = predictions.count{ case(expected, actual) => expected == assignments(actual)}
    success.toDouble / predictions.size.toDouble
  }

  private def mostFrequent[T](list: List[T]): T = {
    list.groupBy(identity).maxBy(_._2.size)._1
  }
}
