package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core._

import scala.io.Source
import breeze.linalg.DenseVector

class HousingDataset extends DatasetSource {

  override def metadata = super.metadata.copy(
    name = "Housing", category = "Dataset", description =
      """
        |This dataset was taken from the StatLib library which is maintained at Carnegie Mellon University.
        |It concerns housing values in suburbs of Boston.
        |
        |The data set can be used for the tasks of regression.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/Housing
      """.stripMargin
  )

  def file: String = HousingDataset.file

  def features: List[(String, FeatureType)] = HousingDataset.featureTypes

}

object HousingDataset {
  val file: String = "/dataset/housing.csv"
  val featureTypes = List(
    "CRIM" -> CONTINUOUS,
    "ZN" -> CONTINUOUS,
    "INDUS" -> CONTINUOUS,
    "CHAS" -> BOOLEAN,
    "NOX" -> CONTINUOUS,
    "RM" -> CONTINUOUS,
    "AGE" -> CONTINUOUS,
    "DIS" -> CONTINUOUS,
    "RAD" -> CONTINUOUS,
    "TAX" -> CONTINUOUS,
    "PTRATIO" -> CONTINUOUS,
    "B" -> CONTINUOUS,
    "LSTAT" -> CONTINUOUS,
    "MEDV" -> CONTINUOUS
  )
  val featureNames = featureTypes.map(_._1)

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map { line =>
    val values = (featureTypes, line.split(",")).zipped.map { case ((name, featureType), raw) => featureType match {
      case CONTINUOUS => (name, raw.toDouble)
      case BOOLEAN => (name, raw == "1")
    }
    }.toMap
    Instance(values)
  }

  def preProcessedIterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map { line =>
    val values = line.split(",").map(_.toDouble)
    val label = (values(13) - stats(13)._1) / stats(13)._2
    val features = DenseVector(values.slice(0, 13))
    for (i <- 0 until 13) {
      val (mean, stddev) = stats(i)
      features(i) = (features(i) - mean) / stddev
    }
    Instance(Map("Label" -> label, "Features" -> features))
  }

  private val stats = Map(
    0 ->(3.6135235573122535, 8.593041351295769),
    1 ->(11.363636363636363, 23.299395694766027),
    2 ->(11.136778656126504, 6.853570583390873),
    3 ->(0.0691699604743083, 0.25374293496034855),
    4 ->(0.5546950592885372, 0.11576311540656153),
    5 ->(6.284634387351787, 0.7019225143345692),
    6 ->(68.57490118577078, 28.121032570236885),
    7 ->(3.795042687747034, 2.103628356344459),
    8 ->(9.549407114624506, 8.698651117790645),
    9 ->(408.2371541501976, 168.3704950393814),
    10 ->(18.455533596837967, 2.162805191482142),
    11 ->(356.67403162055257, 91.20460745217272),
    12 ->(12.653063241106723, 7.134001636650485),
    13 ->(22.532806324110698, 9.188011545278206)
  )
}
