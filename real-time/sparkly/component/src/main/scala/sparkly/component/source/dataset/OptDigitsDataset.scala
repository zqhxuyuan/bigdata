package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance

import scala.io.Source
import breeze.linalg.DenseVector

class OptDigitsDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Optical digits", category = "Dataset", description =
      """
        |Preprocessed bitmaps of handwritten digits.
        |Data is encoded into 64 continuous features.
        |
        |The data set can be used for the tasks of multi-class classification.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Optical+Recognition+of+Handwritten+Digits
      """.stripMargin
  )

  def file: String = OptDigitsDataset.file
  def features: List[(String, FeatureType)] = List("Label" -> INTEGER, "Features" -> VECTOR)
  override def parse(line: String): Instance = OptDigitsDataset.parse(line)
}

object OptDigitsDataset {
  val file: String = "/dataset/optdigits.csv"

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map(line => parse(line))

  def parse(line: String): Instance = {
    val values = line.split(",").map(_.toDouble)
    val label = values(64).toInt
    val features = DenseVector(values.slice(0, 64))

    Instance(Map("Label" -> label, "Features" -> features))
  }
}