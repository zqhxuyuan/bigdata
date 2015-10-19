package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance
import scala.io.Source
import breeze.linalg.DenseVector

class SpamDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Spam", category = "Dataset", description =
      """
        |Spam dataset containing a collection of spam and non-spam emails.
        |Data is encoded into 57 continuous features.
        |
        |The data set can be used for the tasks of classification.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Spambase
      """.stripMargin
  )

  def file: String = SpamDataset.file
  def features: List[(String, FeatureType)] = List("Label" -> BOOLEAN, "Features" -> VECTOR)

  override def parse(line: String): Instance = SpamDataset.parse(line)
}

object SpamDataset {
  val file: String = "/dataset/spam.csv"

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map(line => parse(line))

  def parse(line: String): Instance = {
    val (head :: tail) = line.split(",").toList
    val label = head == "1"
    val features = DenseVector[Double](tail.map(_.toDouble).toArray)

    Instance(Map("Label" -> label, "Features" -> features))
  }
}