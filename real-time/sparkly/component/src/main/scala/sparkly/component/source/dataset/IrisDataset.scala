package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance

import scala.io.Source

class IrisDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Iris", category = "Dataset", description =
      """
        |This is perhaps the best known database to be found in the pattern recognition literature.
        |The data set contains 3 classes of 50 instances each, where each class refers to a type of iris plant.
        |One class is linearly separable from the other 2; the latter are NOT linearly separable from each other.
        |
        |The data set can be used for the tasks of classification.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Iris
      """.stripMargin
  )

  def file: String = IrisDataset.file
  def features: List[(String, FeatureType)] = IrisDataset.featureTypes

}

object IrisDataset {
  val file: String = "/dataset/iris.csv"
  val featureTypes = List (
    "Sepal length" -> CONTINUOUS,
    "Sepal width" -> CONTINUOUS,
    "Petal length" -> CONTINUOUS,
    "Petal width" -> CONTINUOUS,
    "Class" -> CATEGORICAL
  )
  val featureNames = featureTypes.map(_._1)

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map{ line =>
    val values = (featureTypes, line.split(",")).zipped.map{ case((name, featureType), raw) => featureType match {
      case CONTINUOUS => (name, raw.toDouble)
      case CATEGORICAL => (name, raw)
    }}.toMap
    Instance(values)
  }
}