package sparkly.component.source.dataset

import sparkly.core.FeatureType._
import sparkly.core._
import scala.io.Source

class ReutersDataset extends DatasetSource {

  override def metadata = super.metadata.copy (
    name = "Reuters", category = "Dataset", description =
      """
        |This is a sample of the most widely used test collection for text categorization.
        |The data set contains 9 classes of different size.
        |The data was originally collected and labeled by Carnegie Group, Inc. and Reuters, Ltd. in the course of developing the CONSTRUE text categorization system.
        |
        |The data set can be used for text mining tasks.
        |For more information, see http://www.daviddlewis.com/resources/testcollections/reuters21578/readme.txt
      """.stripMargin
  )

  def file: String = ReutersDataset.file
  def features: List[(String, FeatureType)] = List("Topic" -> STRING, "Text" -> STRING)
  override def parse(line: String): Instance = ReutersDataset.parse(line)
}

object ReutersDataset {
  val file: String = "/dataset/reuters.txt"

  def iterator(): Iterator[Instance] = Source.fromInputStream(getClass.getResourceAsStream(file)).getLines().map(line => parse(line))

  def parse(line: String): Instance = {
    val topic = line.split(",")(0)
    val text = line.substring(line.indexOf("-") + 1)
    Instance("Topic" -> topic, "Text" -> text)
  }

}