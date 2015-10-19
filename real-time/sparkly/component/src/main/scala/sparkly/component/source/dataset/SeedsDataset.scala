package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core.Instance
import scala.io.Source
import breeze.linalg.DenseVector

class SeedsDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Seeds", category = "Dataset", description =
      """
        |Measurements of geometrical properties of kernels belonging to three different varieties of wheat.
        |A soft X-ray technique and GRAINS package were used to construct all seven, real-valued attributes.
        |The examined group comprised kernels belonging to three different varieties of wheat: Kama, Rosa and Canadian, 70 elements each.
        |
        |The data set can be used for the tasks of classification and cluster analysis.
        |For more information, see https://archive.ics.uci.edu/ml/datasets/seeds
      """.stripMargin
  )

  def file: String = SeedsDataset.file
  def features: List[(String, FeatureType)] = List("Features" -> VECTOR, "Variety" -> INTEGER)

  override def parse(line: String): Instance = SeedsDataset.parse(line)
}

object SeedsDataset {
  val file = "/dataset/seeds.csv"

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map(line => parse(line))

  def parse(line: String): Instance = {
    val values = line.split(",")
    val label = values(7).toInt
    val features = DenseVector[Double](values.slice(0, 7).map(_.toDouble))

    Instance(Map("Variety" -> label, "Features" -> features))
  }
}