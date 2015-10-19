package sparkly.component.source.dataset

import java.util.Date
import java.net.URL
import java.io._
import java.util.zip.ZipFile

import org.apache.spark.ml.recommendation.ALS.Rating

import collection.JavaConversions.enumerationAsScalaIterator
import sys.process._
import scala.io.Source

import org.apache.spark.streaming.dstream.DStream

import sparkly.core.FeatureType._
import sparkly.core._
import org.apache.commons.io.IOUtils
import org.apache.spark.Logging

class MovieLensDataset extends Component {

  override def metadata = ComponentMetadata (
    name = "MovieLens", category = "Dataset", description =
      """
        |These files contain 1,000,209 anonymous ratings of approximately 3,900 movies made by 6,040 MovieLens users who joined MovieLens in 2000.
        |
        |The data set can be used for recommendation tasks.
        |For more information, see http://grouplens.org/datasets/movielens/
      """.stripMargin,
    outputs = Map ("Ratings" -> OutputStreamMetadata(namedFeatures = Map("User" -> LONG, "Item" -> LONG, "Rating" -> INTEGER, "Timestamp" -> DATE))),
    properties = Map (
      "Throughput (instance/second)" -> PropertyMetadata(PropertyType.INTEGER, defaultValue = Some(1000)),
      "Loop" -> PropertyMetadata(PropertyType.BOOLEAN, defaultValue = Some(true)),
      "Size" -> PropertyMetadata(PropertyType.STRING, defaultValue = Some("1m"), acceptedValues = List("1m", "10m"))
    )
  )

  override protected def initStreams(context: Context): Map[String, DStream[Instance]] = {
    val throughput = context.property("Throughput (instance/second)").as[Int]
    val loop = context.property("Loop").as[Boolean]
    val size = context.property("Size").as[String]
    val file = MovieLensDataset.getFile(size)
    val stream: DStream[Instance] = context.ssc.receiverStream(new DatasetReceiver(file, MovieLensDataset.parse, throughput, loop))
    Map("Ratings" -> stream)
  }

}

object MovieLensDataset extends Logging {

  private val expectedEntries = Map (
    "1m" -> "ml-1m/ratings.dat",
    "10m" -> "ml-10M100K/ratings.dat"
  )

  def iterator(size: String): Iterator[Instance] = {
    val file = getFile(size)
    Source.fromFile(file).getLines().map(line => parse(line))
  }

  def trainDataset(): List[Instance] = Source.fromURL("http://files.grouplens.org/datasets/movielens/ml-100k/u1.base").getLines().map{ line =>
    val values = line.split("\t")
    val user = values(0).toLong
    val item = values(1).toLong
    val rating = values(2).toDouble
    Instance("User" -> user, "Item" -> item, "Rating" -> rating)
  }.toList

  def testDataset(): List[Instance] = Source.fromURL("http://files.grouplens.org/datasets/movielens/ml-100k/u1.test").getLines().map{ line =>
    val values = line.split("\t")
    val user = values(0).toLong
    val item = values(1).toLong
    val rating = values(2).toDouble
    Instance("User" -> user, "Item" -> item, "Rating" -> rating)
  }.toList

  def parse(line: String): Instance = {
    val values = line.split("::")
    val user = values(0).toLong
    val item = values(1).toLong
    val rating = values(2).toInt
    val timestamp = new Date(values(3).toLong)
    Instance("User" -> user, "Item" -> item, "Rating" -> rating, "Timestamp" -> timestamp)
  }

  def getFile(size: String): String = {
    val file = s"/tmp/movielens-$size.data"
    if(!new File(file).exists) {
      download(size, file)
    }
    file
  }

  private def download(size: String, destination: String): Unit = {
    val zipDestination = s"/tmp/movielens-$size.zip"

    val url = s"http://files.grouplens.org/datasets/movielens/ml-$size.zip"
    logInfo(s"Downloading $url")
    new URL(url).#>(new File(zipDestination)).!!

    val zipFile = new ZipFile(zipDestination)
    zipFile.entries().find(e => e.getName == expectedEntries(size)).foreach { entry =>
      logDebug(s"Writing ${entry.getName} to $destination")
      IOUtils.copy(zipFile.getInputStream(entry), new FileOutputStream(destination))
    }
  }
}